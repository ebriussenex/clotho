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
    app::{Command, CommandError, CommandResponse, ReplyContext, Request},
    message::Outgoing,
};

use super::{
    pending::{PendingTable, Reply},
    reply_cache::{Admission, ReplyCache},
    request::{OutboundRequests, PeerRequest},
};

/// Errors of internode requests and of writing replies.
#[derive(Debug, Error)]
pub enum TransportErr {
    /// A reply arrived for a `msg_id` nobody waits for: the request already completed, timed
    /// out or was cancelled. Harmless, the reply is dropped.
    #[error("got response which was not awaited")]
    NotAwaited,
    /// The peer did not answer before [`RpcConfig::deadline`]; the request may or may not have
    /// been executed.
    #[error("timed out waiting for {0}")]
    Timeout(&'static str),
    /// The reply channel closed. Cannot happen while [`Transport::forward`] holds its sender.
    #[error("response sender dropped: {0}")]
    SenderDropped(&'static str),
    /// The node is stopping and the request was abandoned; the peer may or may not have executed
    /// it.
    #[error("transport cancelled")]
    Cancelled,
    /// The output task is gone, so the node can't send anything. Fatal.
    #[error("failed to send to {channel} channel")]
    ChanSend { channel: &'static str },
    /// The peer replied with a non-retryable error, passed on to the client as is.
    #[error("peer replied with a non-retryable error: {0:?}")]
    Remote(CommandError),
    /// The peer replied with a response to another kind of command. A bug on either node; the
    /// request may or may not have been executed.
    #[error("peer replied with a response of a wrong type")]
    UnexpectedResponse,
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

/// Retry policy of [`Transport::forward`].
#[derive(Debug, Clone, Copy)]
pub struct RpcConfig {
    /// How long one attempt waits for its reply before the request is sent again.
    pub attempt_timeout: Duration,
    /// Total time for all attempts of one request.
    pub deadline: Duration,
    /// Shortest pause between attempts; the pause grows exponentially, with jitter.
    pub min_backoff: Duration,
    /// Longest pause between attempts.
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

/// Outcome of [`Transport::deduplicate`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Delivery {
    /// A new request: execute it.
    First,
    /// A retry of a known request: already answered from the cache or dropped.
    Duplicate,
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

    /// Sends `request` to the peer `dest` and waits for its reply, retrying until
    /// [`RpcConfig::deadline`].
    ///
    /// Every attempt carries the same request id, so the peer executes the command at most
    /// once and answers retries with the remembered reply. A late reply to any earlier attempt
    /// completes the request. [`ErrorKind::Unavailable`](crate::app::ErrorKind::Unavailable)
    /// replies are retried like timeouts.
    ///
    /// # Errors
    ///
    /// - [`TransportErr::Timeout`] if no reply came before the deadline.
    /// - [`TransportErr::Remote`] if the peer replied with a non-retryable error.
    /// - [`TransportErr::UnexpectedResponse`] if the reply is not a `Q::Response`.
    /// - [`TransportErr::Cancelled`] if `token` was cancelled.
    /// - [`TransportErr::ChanSend`] if the output task is gone.
    pub async fn forward<Q: Request>(
        &self,
        token: &CancellationToken,
        dest: &str,
        request: Q,
    ) -> Result<Q::Response, TransportErr> {
        let response = self.forward_command(token, dest, request.into()).await?;
        Q::Response::try_from(response).map_err(|response| {
            tracing::error!(
                dest,
                ?response,
                "peer replied with a response of a wrong type"
            );
            TransportErr::UnexpectedResponse
        })
    }

    async fn forward_command(
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

    /// Commands from clients and read-only commands from peers are always [`Delivery::First`].
    /// A retry of a peer write that already completed is answered here with the remembered
    /// reply; a retry of one still in progress, or of one the peer no longer waits for, is
    /// dropped.
    ///
    /// # Errors
    ///
    /// [`TransportErr::ChanSend`] if replaying a remembered reply fails because the output task
    /// is gone.
    pub async fn deduplicate(&self, reply_ctx: &ReplyContext) -> Result<Delivery, TransportErr> {
        let Some(request) = deduplicated(reply_ctx.request) else {
            return Ok(Delivery::First);
        };

        match self.replies.admit(&reply_ctx.dest, request) {
            Admission::Execute => Ok(Delivery::First),
            Admission::Replay(response) => {
                tracing::debug!(origin = %reply_ctx.dest, seq = request.id.seq, "replaying reply to a retried request");
                self.write(Outgoing::ForwardReply {
                    msg_id: self.next_msg_id(),
                    // deep copy only if the cache still holds it, and outside the cache lock
                    response: Arc::unwrap_or_clone(response),
                    in_reply_to: reply_ctx.in_reply_to,
                    dest: reply_ctx.dest.clone(),
                })
                .await?;
                Ok(Delivery::Duplicate)
            }
            Admission::InProgress => {
                tracing::debug!(origin = %reply_ctx.dest, seq = request.id.seq, "dropping retry of a request in progress");
                Ok(Delivery::Duplicate)
            }
            Admission::Stale => {
                tracing::debug!(origin = %reply_ctx.dest, seq = request.id.seq, "dropping request no longer awaited");
                Ok(Delivery::Duplicate)
            }
        }
    }

    /// Sends a successful reply. For a deduplicated peer request the reply is also remembered,
    /// so a retry of the request is answered without executing it again.
    ///
    /// # Errors
    ///
    /// [`TransportErr::ChanSend`] if the output task is gone.
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

        let msg_id = self.next_msg_id();
        let outgoing = match request {
            None => Outgoing::CommandReply {
                msg_id,
                response,
                in_reply_to,
                dest,
            },
            Some(request) => {
                if request.deduplicate {
                    self.replies
                        .complete(&dest, request.id, Arc::new(response.clone()));
                }
                Outgoing::ForwardReply {
                    msg_id,
                    response,
                    in_reply_to,
                    dest,
                }
            }
        };
        self.write(outgoing).await
    }

    /// Sends an error reply. A peer request is forgotten, so its retry is executed again.
    ///
    /// # Errors
    ///
    /// [`TransportErr::ChanSend`] if the output task is gone.
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

        if let Some(request) = deduplicated(request) {
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

    /// Hands a peer's reply to the request waiting for it; a reply nobody waits for is dropped.
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

fn deduplicated(request: Option<PeerRequest>) -> Option<PeerRequest> {
    request.filter(|request| request.deduplicate)
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::sync::mpsc;
    use tokio_util::{bytes::Bytes, sync::CancellationToken};

    use crate::{
        app::{
            CommandError, CommandResponse, CommitOffsetsRes, ErrorKind, ReplyContext, SendCommand,
            SendRes,
        },
        message::Outgoing,
        transport::{PeerRequest, RequestId},
    };

    use super::{Delivery, RpcConfig, Transport, TransportErr};

    fn config() -> RpcConfig {
        // short real intervals: tokio's paused clock needs the `test-util` feature
        RpcConfig {
            attempt_timeout: Duration::from_millis(20),
            deadline: Duration::from_millis(200),
            min_backoff: Duration::from_millis(5),
            max_backoff: Duration::from_millis(5),
        }
    }

    fn send_command() -> SendCommand {
        SendCommand {
            key: "log".to_owned(),
            msg: Bytes::from_static(b"7"),
        }
    }

    fn spawn_forward(
        transport: &Transport,
        token: &CancellationToken,
    ) -> tokio::task::JoinHandle<Result<SendRes, TransportErr>> {
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
        assert!(matches!(response, Ok(SendRes { offset: 4 })));
    }

    #[tokio::test]
    async fn forward_rejects_response_of_wrong_type() {
        let (write_tx, mut write_rx) = mpsc::channel(4);
        let transport = Transport::new(write_tx, config(), 1);
        let task = spawn_forward(&transport, &CancellationToken::new());

        let (msg_id, _) = next_attempt(&mut write_rx).await;
        transport.register_reply(msg_id, Ok(CommandResponse::CommitOffsets(CommitOffsetsRes)));

        let response = task.await.expect("forward task must complete");
        assert!(matches!(response, Err(TransportErr::UnexpectedResponse)));
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
        assert!(matches!(response, Ok(SendRes { offset: 9 })));
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
                deduplicate: true,
            }),
        }
    }

    fn read_only_peer_ctx(in_reply_to: u64) -> ReplyContext {
        let mut ctx = peer_ctx(in_reply_to);
        if let Some(request) = ctx.request.as_mut() {
            request.deduplicate = false;
        }
        ctx
    }

    #[tokio::test]
    async fn retry_of_executed_peer_request_gets_remembered_reply() {
        let (write_tx, mut write_rx) = mpsc::channel(4);
        let transport = Transport::new(write_tx, config(), 1);

        assert_eq!(delivery(&transport, peer_ctx(10)).await, Delivery::First);
        transport
            .reply(CommandResponse::Send(SendRes { offset: 3 }), peer_ctx(10))
            .await
            .expect("reply");
        assert!(matches!(
            write_rx.recv().await,
            Some(Outgoing::ForwardReply {
                in_reply_to: 10,
                ..
            })
        ));

        // the retry is not executed, the reply is sent again to the new attempt
        assert_eq!(
            delivery(&transport, peer_ctx(11)).await,
            Delivery::Duplicate
        );
        assert!(matches!(
            write_rx.recv().await,
            Some(Outgoing::ForwardReply {
                in_reply_to: 11,
                response: CommandResponse::Send(SendRes { offset: 3 }),
                ..
            })
        ));
    }

    #[tokio::test]
    async fn retry_of_read_only_peer_request_is_executed_again() {
        let (write_tx, mut write_rx) = mpsc::channel(4);
        let transport = Transport::new(write_tx, config(), 1);

        assert_eq!(
            delivery(&transport, read_only_peer_ctx(10)).await,
            Delivery::First
        );
        // a retry while the first attempt is still running is executed too
        assert_eq!(
            delivery(&transport, read_only_peer_ctx(11)).await,
            Delivery::First
        );
        transport
            .reply(
                CommandResponse::CommitOffsets(CommitOffsetsRes),
                read_only_peer_ctx(10),
            )
            .await
            .expect("reply");
        assert!(matches!(
            write_rx.recv().await,
            Some(Outgoing::ForwardReply {
                in_reply_to: 10,
                ..
            })
        ));

        // nothing was remembered: a later retry is executed again, nothing is replayed
        assert_eq!(
            delivery(&transport, read_only_peer_ctx(12)).await,
            Delivery::First
        );
        assert!(write_rx.try_recv().is_err());
    }

    async fn delivery(transport: &Transport, reply_ctx: ReplyContext) -> Delivery {
        transport
            .deduplicate(&reply_ctx)
            .await
            .expect("deduplicated")
    }
}
