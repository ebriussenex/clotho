use std::sync::{
    Arc,
    atomic::{self, AtomicU64},
};

use thiserror::Error;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::{
    app::{Command, CommandResponse, ReplyContext},
    message::Outgoing,
    replication::ReplicationMessage,
};

use super::pending::PendingTable;

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

#[derive(Clone)]
pub struct Transport {
    msg_id: Arc<AtomicU64>,
    write_tx: mpsc::Sender<Outgoing>,
    timeout: tokio::time::Duration,
    pending: PendingTable,
}

impl Transport {
    pub fn new(write_tx: mpsc::Sender<Outgoing>, timeout: tokio::time::Duration) -> Self {
        Self {
            msg_id: Arc::new(AtomicU64::new(1)),
            write_tx,
            timeout,
            pending: PendingTable::new(),
        }
    }

    pub async fn forward(
        &self,
        token: &CancellationToken,
        dest: &str,
        command: Command,
    ) -> Result<CommandResponse, TransportErr> {
        let msg_id = self.next_msg_id();
        let response_rx = self.pending.add(msg_id).await;
        let outgoing = Outgoing::Forward {
            command,
            msg_id,
            dest: dest.to_owned(),
        };

        if self.write_tx.send(outgoing).await.is_err() {
            self.pending.remove(msg_id).await;
            return Err(TransportErr::chan_send::<Outgoing>("write_tx"));
        }

        tokio::select! {
            biased;
            _ = token.cancelled() => {
                self.pending.remove(msg_id).await;
                Err(TransportErr::Cancelled)
            }
            response = response_rx => {
                response.map_err(|_| TransportErr::SenderDropped("command_reply_tx"))
            }
            _ = tokio::time::sleep(self.timeout) => {
                self.pending.remove(msg_id).await;
                Err(TransportErr::Timeout("command reply"))
            }
        }
    }

    pub async fn reply(
        &self,
        response: CommandResponse,
        reply_ctx: ReplyContext,
    ) -> Result<(), TransportErr> {
        let ReplyContext { in_reply_to, dest } = reply_ctx;
        self.write_tx
            .send(Outgoing::CommandReply {
                response,
                msg_id: self.next_msg_id(),
                in_reply_to,
                dest,
            })
            .await
            .map_err(|_| TransportErr::chan_send::<CommandResponse>("write_tx"))
    }

    pub async fn register_reply(
        &self,
        in_reply_to: usize,
        response: CommandResponse,
    ) -> Result<(), TransportErr> {
        match self.pending.register(in_reply_to, response).await {
            Err(TransportErr::NotAwaited) => {
                tracing::warn!(
                    msg_id = in_reply_to,
                    "ignoring reply without pending forward"
                );
                Ok(())
            }
            result => result,
        }
    }

    pub async fn send_replication(
        &self,
        dest: &str,
        message: ReplicationMessage,
    ) -> Result<(), TransportErr> {
        self.write_tx
            .send(Outgoing::Replication {
                message,
                msg_id: self.next_msg_id(),
                dest: dest.to_owned(),
            })
            .await
            .map_err(|_| TransportErr::chan_send::<ReplicationMessage>("write_tx"))
    }

    fn next_msg_id(&self) -> usize {
        self.msg_id.fetch_add(1, atomic::Ordering::Relaxed) as usize
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::sync::mpsc;
    use tokio_util::sync::CancellationToken;

    use crate::{
        app::{Command, CommandResponse, SendCommand, SendRes},
        domain::{LogId, Offset},
        message::Outgoing,
    };

    use super::Transport;

    #[tokio::test]
    async fn forward_resolves_matching_reply() {
        let (write_tx, mut write_rx) = mpsc::channel(1);
        let transport = Transport::new(write_tx, Duration::from_secs(1));
        let task = tokio::spawn({
            let transport = transport.clone();
            async move {
                transport
                    .forward(
                        &CancellationToken::new(),
                        "n2",
                        Command::Send(SendCommand {
                            key: LogId("log".to_owned()),
                            msg: 7,
                        }),
                    )
                    .await
            }
        });

        let msg_id = match write_rx.recv().await {
            Some(Outgoing::Forward { msg_id, dest, .. }) => {
                assert_eq!(dest, "n2");
                msg_id
            }
            _ => panic!("expected a forwarded command"),
        };

        transport
            .register_reply(msg_id, CommandResponse::Send(SendRes { offset: Offset(4) }))
            .await
            .expect("matching reply must be registered");

        let response = task
            .await
            .expect("forward task must complete")
            .expect("forward must receive its reply");

        assert!(matches!(
            response,
            CommandResponse::Send(SendRes { offset: Offset(4) })
        ));
    }

    #[tokio::test]
    async fn forward_observes_cancellation() {
        let (write_tx, mut write_rx) = mpsc::channel(1);
        let transport = Transport::new(write_tx, Duration::from_secs(1));
        let token = CancellationToken::new();
        let task = tokio::spawn({
            let transport = transport.clone();
            let token = token.clone();
            async move {
                transport
                    .forward(
                        &token,
                        "n2",
                        Command::Send(SendCommand {
                            key: LogId("log".to_owned()),
                            msg: 7,
                        }),
                    )
                    .await
            }
        });

        assert!(matches!(
            write_rx.recv().await,
            Some(Outgoing::Forward { .. })
        ));
        token.cancel();

        let result = task.await.expect("forward task must complete");
        assert!(matches!(result, Err(super::TransportErr::Cancelled)));
    }
}
