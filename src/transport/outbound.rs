use std::{
    sync::{
        Arc,
        atomic::{self, AtomicU64},
    },
    time::Duration,
};

use backon::{BackoffBuilder, ExponentialBuilder};
use thiserror::Error;
use tokio::{sync::mpsc, time::Instant};
use tokio_util::sync::CancellationToken;

use crate::{
    app::{Command, CommandError, CommandResponse, ReplyContext},
    message::Outgoing,
};

use super::{
    pending::{PendingTable, Reply},
    reply_cache::{Admission, ReplyCache},
    request::OutboundRequests,
};

#[derive(Debug, Error)]
pub enum TransportErr {
    #[error("got response which was not awaited")]
    NotAwaited,
    #[error("timed out waiting for {0}")]
    Timeout(&'static str),
    #[error("response sender dropped: {0}")]
    SenderDropped(&'static str),
    #[error("transport cancelled")]
    Cancelled,
    #[error("failed to send to {channel} channel")]
    ChanSend { channel: &'static str },
    #[error("peer replied with a non-retryable error: {0:?}")]
    Remote(CommandError),
}

impl TransportErr {
    pub fn chan_send<T>(channel: &'static str) -> Self {
        tracing::error!(
            channel,
            payload_type = std::any::type_name::<T>(),
            "channel send failed"
        );
        Self::ChanSend { channel }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct RpcConfig {
    pub attempt_timeout: Duration,
    pub deadline: Duration,
    pub min_backoff: Duration,
    pub max_backoff: Duration,
}

impl Default for RpcConfig {
    fn default() -> Self {
        Self {
            attempt_timeout: Duration::from_millis(300),
            deadline: Duration::from_secs(2),
            min_backoff: Duration::from_millis(20),
            max_backoff: Duration::from_millis(200),
        }
    }
}

impl RpcConfig {
    fn backoff(&self) -> ExponentialBuilder {
        ExponentialBuilder::new()
            .with_min_delay(self.min_backoff)
            .with_max_delay(self.max_backoff)
            .with_jitter()
            .without_max_times()
    }
}

#[derive(Clone)]
pub struct Transport {
    msg_id: Arc<AtomicU64>,
    write_tx: mpsc::Sender<Outgoing>,
    config: RpcConfig,
    pending: PendingTable,
    outbound: Arc<OutboundRequests>,
    replies: Arc<ReplyCache>,
}

enum Waited {
    Done(Result<CommandResponse, TransportErr>),
    Retry,
}

impl Transport {
    pub fn new(write_tx: mpsc::Sender<Outgoing>, config: RpcConfig, incarnation: u64) -> Self {
        Self {
            msg_id: Arc::new(AtomicU64::new(1)),
            write_tx,
            config,
            pending: PendingTable::new(),
            outbound: Arc::new(OutboundRequests::new(incarnation)),
            replies: Arc::new(ReplyCache::default()),
        }
    }

    pub async fn forward(
        &self,
        token: &CancellationToken,
        dest: &str,
        command: Command,
    ) -> Result<CommandResponse, TransportErr> {
        let request = self.outbound.begin();
        let (reply_tx, mut reply_rx) = mpsc::unbounded_channel();
        let mut attempts = self.pending.attempts();
        let deadline = Instant::now() + self.config.deadline;
        let mut backoff = self.config.backoff().build();
        let mut attempt: u32 = 0;

        loop {
            attempt = attempt.saturating_add(1);
            let msg_id = self.next_msg_id();
            attempts.add(msg_id, reply_tx.clone());
            self.write(Outgoing::Forward {
                command: command.clone(),
                msg_id,
                dest: dest.to_owned(),
                request_id: request.id(),
                watermark: self.outbound.watermark(),
            })
            .await?;

            let attempt_end = deadline.min(Instant::now() + self.config.attempt_timeout);
            if let Waited::Done(result) = wait_reply(token, &mut reply_rx, attempt_end, true).await
            {
                return result;
            }

            // Pause before the next attempt, still accepting a late reply to earlier ones.
            let now = Instant::now();
            let retry_at = backoff
                .next()
                .map_or(deadline, |delay| deadline.min(now + delay));
            if let Waited::Done(result) = wait_reply(token, &mut reply_rx, retry_at, false).await {
                return result;
            }

            if Instant::now() >= deadline {
                tracing::warn!(
                    dest,
                    seq = request.id().seq,
                    attempts = attempt,
                    "forward deadline exceeded"
                );
                return Err(TransportErr::Timeout("forward reply"));
            }
            tracing::debug!(dest, seq = request.id().seq, attempt, "retrying forward");
        }
    }

    /// Returns whether the command must be executed.
    pub async fn admit(&self, reply_ctx: &ReplyContext) -> Result<bool, TransportErr> {
        let Some(request) = reply_ctx.request else {
            return Ok(true);
        };

        match self.replies.admit(&reply_ctx.dest, request) {
            Admission::Execute => Ok(true),
            Admission::Replay(response) => {
                tracing::debug!(origin = %reply_ctx.dest, seq = request.id.seq, "replaying reply to a retried request");
                self.write(Outgoing::CommandReply {
                    msg_id: self.next_msg_id(),
                    // deep copy only if the cache still holds it, and outside the cache lock
                    response: Arc::unwrap_or_clone(response),
                    in_reply_to: reply_ctx.in_reply_to,
                    dest: reply_ctx.dest.clone(),
                })
                .await?;
                Ok(false)
            }
            Admission::InProgress => {
                tracing::debug!(origin = %reply_ctx.dest, seq = request.id.seq, "dropping retry of a request in progress");
                Ok(false)
            }
            Admission::Stale => {
                tracing::debug!(origin = %reply_ctx.dest, seq = request.id.seq, "dropping request no longer awaited");
                Ok(false)
            }
        }
    }

    pub async fn reply(
        &self,
        response: CommandResponse,
        reply_ctx: ReplyContext,
    ) -> Result<(), TransportErr> {
        let ReplyContext {
            dest,
            in_reply_to,
            request,
        } = reply_ctx;

        if let Some(request) = request {
            // copied outside the cache lock
            self.replies
                .complete(&dest, request.id, Arc::new(response.clone()));
        }

        self.write(Outgoing::CommandReply {
            msg_id: self.next_msg_id(),
            response,
            in_reply_to,
            dest,
        })
        .await
    }

    pub async fn reply_error(
        &self,
        error: CommandError,
        reply_ctx: ReplyContext,
    ) -> Result<(), TransportErr> {
        let ReplyContext {
            dest,
            in_reply_to,
            request,
        } = reply_ctx;

        if let Some(request) = request {
            self.replies.abandon(&dest, request.id);
        }

        self.write(Outgoing::ErrorReply {
            msg_id: self.next_msg_id(),
            error,
            in_reply_to,
            dest,
        })
        .await
    }

    pub fn register_reply(&self, in_reply_to: u64, reply: Reply) {
        if let Err(TransportErr::NotAwaited) = self.pending.register(in_reply_to, reply) {
            tracing::debug!(msg_id = in_reply_to, "ignoring reply nobody waits for");
        }
    }

    async fn write(&self, outgoing: Outgoing) -> Result<(), TransportErr> {
        self.write_tx
            .send(outgoing)
            .await
            .map_err(|_| TransportErr::chan_send::<Outgoing>("write_tx"))
    }

    fn next_msg_id(&self) -> u64 {
        self.msg_id.fetch_add(1, atomic::Ordering::Relaxed)
    }
}

async fn wait_reply(
    token: &CancellationToken,
    reply_rx: &mut mpsc::UnboundedReceiver<Reply>,
    until: Instant,
    stop_on_retryable: bool,
) -> Waited {
    loop {
        tokio::select! {
            biased;
            _ = token.cancelled() => return Waited::Done(Err(TransportErr::Cancelled)),
            reply = reply_rx.recv() => match reply {
                Some(Ok(response)) => return Waited::Done(Ok(response)),
                Some(Err(error)) if error.kind.is_retryable() => {
                    tracing::debug!(?error, "peer is temporarily unavailable");
                    if stop_on_retryable {
                        return Waited::Retry;
                    }
                }
                Some(Err(error)) => return Waited::Done(Err(TransportErr::Remote(error))),
                None => return Waited::Done(Err(TransportErr::SenderDropped("forward reply"))),
            },
            _ = tokio::time::sleep_until(until) => return Waited::Retry,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::sync::mpsc;
    use tokio_util::sync::CancellationToken;

    use crate::{
        app::{
            Command, CommandError, CommandResponse, ErrorKind, ReplyContext, SendCommand, SendRes,
        },
        message::Outgoing,
        transport::{PeerRequest, RequestId},
    };

    use super::{RpcConfig, Transport, TransportErr};

    fn config() -> RpcConfig {
        // short real intervals: tokio's paused clock needs the `test-util` feature
        RpcConfig {
            attempt_timeout: Duration::from_millis(20),
            deadline: Duration::from_millis(200),
            min_backoff: Duration::from_millis(5),
            max_backoff: Duration::from_millis(5),
        }
    }

    fn send_command() -> Command {
        Command::Send(SendCommand {
            key: "log".to_owned(),
            msg: 7,
        })
    }

    fn spawn_forward(
        transport: &Transport,
        token: &CancellationToken,
    ) -> tokio::task::JoinHandle<Result<CommandResponse, TransportErr>> {
        let transport = transport.clone();
        let token = token.clone();
        tokio::spawn(async move { transport.forward(&token, "n2", send_command()).await })
    }

    /// Returns `(msg_id, request_id)` of the next forwarded attempt.
    async fn next_attempt(write_rx: &mut mpsc::Receiver<Outgoing>) -> (u64, RequestId) {
        match write_rx.recv().await {
            Some(Outgoing::Forward {
                msg_id,
                request_id,
                dest,
                ..
            }) => {
                assert_eq!(dest, "n2");
                (msg_id, request_id)
            }
            _ => panic!("expected a forwarded command"),
        }
    }

    #[tokio::test]
    async fn forward_resolves_matching_reply() {
        let (write_tx, mut write_rx) = mpsc::channel(4);
        let transport = Transport::new(write_tx, config(), 1);
        let task = spawn_forward(&transport, &CancellationToken::new());

        let (msg_id, _) = next_attempt(&mut write_rx).await;
        transport.register_reply(msg_id, Ok(CommandResponse::Send(SendRes { offset: 4 })));

        let response = task.await.expect("forward task must complete");
        assert!(matches!(
            response,
            Ok(CommandResponse::Send(SendRes { offset: 4 }))
        ));
    }

    #[tokio::test]
    async fn forward_retries_with_same_request_id() {
        let (write_tx, mut write_rx) = mpsc::channel(4);
        let transport = Transport::new(write_tx, config(), 1);
        let task = spawn_forward(&transport, &CancellationToken::new());

        let (first_msg_id, first_request) = next_attempt(&mut write_rx).await;
        // no reply: the attempt times out and is retried
        let (second_msg_id, second_request) = next_attempt(&mut write_rx).await;
        assert_ne!(first_msg_id, second_msg_id);
        assert_eq!(first_request, second_request);

        // a late reply to the first attempt still completes the request
        transport.register_reply(
            first_msg_id,
            Ok(CommandResponse::Send(SendRes { offset: 9 })),
        );

        let response = task.await.expect("forward task must complete");
        assert!(matches!(
            response,
            Ok(CommandResponse::Send(SendRes { offset: 9 }))
        ));
    }

    #[tokio::test]
    async fn forward_retries_unavailable_and_fails_on_other_errors() {
        let (write_tx, mut write_rx) = mpsc::channel(4);
        let transport = Transport::new(write_tx, config(), 1);
        let task = spawn_forward(&transport, &CancellationToken::new());

        let (msg_id, _) = next_attempt(&mut write_rx).await;
        transport.register_reply(
            msg_id,
            Err(CommandError::new(ErrorKind::Unavailable, "shutting down")),
        );

        let (msg_id, _) = next_attempt(&mut write_rx).await;
        transport.register_reply(msg_id, Err(CommandError::new(ErrorKind::Protocol, "bad")));

        let result = task.await.expect("forward task must complete");
        assert!(matches!(
            result,
            Err(TransportErr::Remote(CommandError {
                kind: ErrorKind::Protocol,
                ..
            }))
        ));
    }

    #[tokio::test]
    async fn forward_times_out_after_deadline() {
        let (write_tx, mut write_rx) = mpsc::channel(64);
        let transport = Transport::new(write_tx, config(), 1);
        let task = spawn_forward(&transport, &CancellationToken::new());

        let result = task.await.expect("forward task must complete");
        assert!(matches!(result, Err(TransportErr::Timeout(_))));

        // every attempt was sent, all with the same request id
        let (_, request) = next_attempt(&mut write_rx).await;
        let mut attempts = 1;
        while let Ok(Outgoing::Forward { request_id, .. }) = write_rx.try_recv() {
            assert_eq!(request_id, request);
            attempts += 1;
        }
        assert!(attempts > 1);
    }

    #[tokio::test]
    async fn forward_observes_cancellation() {
        let (write_tx, mut write_rx) = mpsc::channel(4);
        let transport = Transport::new(write_tx, config(), 1);
        let token = CancellationToken::new();
        let task = spawn_forward(&transport, &token);

        next_attempt(&mut write_rx).await;
        token.cancel();

        let result = task.await.expect("forward task must complete");
        assert!(matches!(result, Err(TransportErr::Cancelled)));
    }

    fn peer_ctx(in_reply_to: u64) -> ReplyContext {
        ReplyContext {
            dest: "n1".to_owned(),
            in_reply_to,
            request: Some(PeerRequest {
                id: RequestId {
                    incarnation: 1,
                    seq: 0,
                },
                watermark: 0,
            }),
        }
    }

    #[tokio::test]
    async fn retry_of_executed_peer_request_gets_remembered_reply() {
        let (write_tx, mut write_rx) = mpsc::channel(4);
        let transport = Transport::new(write_tx, config(), 1);

        assert!(transport.admit(&peer_ctx(10)).await.expect("admit"));
        transport
            .reply(CommandResponse::Send(SendRes { offset: 3 }), peer_ctx(10))
            .await
            .expect("reply");
        assert!(matches!(
            write_rx.recv().await,
            Some(Outgoing::CommandReply {
                in_reply_to: 10,
                ..
            })
        ));

        // the retry is not executed, the reply is sent again to the new attempt
        assert!(!transport.admit(&peer_ctx(11)).await.expect("admit"));
        assert!(matches!(
            write_rx.recv().await,
            Some(Outgoing::CommandReply {
                in_reply_to: 11,
                response: CommandResponse::Send(SendRes { offset: 3 }),
                ..
            })
        ));
    }
}
