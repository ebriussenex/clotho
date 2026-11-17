use crate::{
    app::{App, AppCommand, AppErr, CommandError, ErrorKind, Ownership, ReplyContext},
    io::{IoError, MessageReader, MessageWriter},
    logs::{Store, StoreConfig},
    maelstrom::protocol::wire::{Msg, Payload},
    message::{self, DropReason, Incoming},
    offsets::CommittedOffsets,
    transport::{self, Delivery, RpcConfig, Transport, TransportErr},
};

use std::{sync::Arc, time::Duration};

use thiserror::Error;
use tokio::{
    signal::unix::{SignalKind, signal},
    sync::mpsc::{self, error::TrySendError},
    task::{JoinError, JoinSet},
};
use tokio_util::sync::CancellationToken;

/// Error that stopped the node.
#[derive(Debug, Error)]
pub enum NodeErr {
    /// SIGINT or SIGTERM can't be listened to, so the node could not be stopped gracefully.
    /// Returned before the node starts.
    #[error("can't listen for stop signals: {0}")]
    Signal(#[source] std::io::Error),
    /// One of the node's tasks failed.
    #[error(transparent)]
    Handle(#[from] HandleErr),
}

/// Failure of a task spawned by [`run`]: [`receive_and_dispatch`], [`write`](fn@write) or
/// [`App::run`]. Stops the whole node.
#[derive(Debug, Error)]
pub enum HandleErr {
    /// A task spawned by [`run`] ended without returning a result: it panicked or was aborted.
    #[error("node task ended abnormally: {0}")]
    Join(#[from] JoinError),
    /// The app failed.
    #[error(transparent)]
    App(#[from] AppErr),
    /// A reply couldn't be sent: the output task is gone.
    #[error(transparent)]
    Transport(#[from] TransportErr),
    /// Reading or writing the stream failed. Malformed messages are not errors, they are
    /// answered with a protocol error and skipped.
    #[error(transparent)]
    Io(#[from] IoError),
}

pub struct Config {
    poll_max_msgs_size: usize,
    rpc: RpcConfig,
    max_in_flight_commands: usize,
    command_queue_capacity: usize,
    ownership_seed: u64,
    /// How long the node may take to stop once cancelled; then its tasks are aborted.
    shutdown_grace: Duration,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            poll_max_msgs_size: 100,
            rpc: RpcConfig::default(),
            max_in_flight_commands: 64,
            command_queue_capacity: 32,
            ownership_seed: 0,
            shutdown_grace: Duration::from_secs(10),
        }
    }
}

/// # Errors
///
/// - [`NodeErr::Signal`] if SIGINT or SIGTERM can't be listened to; the node doesn't start.
/// - [`NodeErr::Handle`] with the first error of a task, returned after the node stopped.
pub async fn run<R, W>(
    config: Config,
    node_id: &str,
    peer_ids: &[String],
    reader: R,
    writer: W,
) -> Result<(), NodeErr>
where
    R: MessageReader,
    W: MessageWriter,
{
    let mut interrupt = signal(SignalKind::interrupt()).map_err(NodeErr::Signal)?;
    let mut terminate = signal(SignalKind::terminate()).map_err(NodeErr::Signal)?;

    let (write_tx, write_rx) = mpsc::channel(32);
    let (command_tx, command_rx) = mpsc::channel(config.command_queue_capacity.max(1));

    let mut node_tasks = JoinSet::new();
    let token = CancellationToken::new();
    let transport = Transport::new(write_tx, config.rpc, transport::new_incarnation());
    let logs = Store::new(StoreConfig::default(), token.child_token());
    let committed_offsets = CommittedOffsets::new();
    let ownership = Ownership::new(
        node_id.to_owned(),
        peer_ids.iter().cloned(),
        config.ownership_seed,
    );

    let node_id: Arc<str> = Arc::from(node_id);
    let shutdown_grace = config.shutdown_grace;

    node_tasks.spawn({
        let token = token.clone();
        let transport = transport.clone();
        async move { receive_and_dispatch(token, command_tx, transport, reader).await }
    });

    node_tasks.spawn(async move { write(writer, write_rx, node_id.as_ref()).await });

    // NOTE: the app gets the last `transport`: the writer finishes once the reader and the app are gone
    node_tasks.spawn({
        let token = token.clone();

        async move {
            App::new(
                logs,
                transport,
                committed_offsets,
                ownership,
                config.poll_max_msgs_size,
                config.max_in_flight_commands,
            )
            .run(token, command_rx)
            .await
            .map_err(HandleErr::from)
        }
    });

    let outcome: Result<(), HandleErr> = tokio::select! {
        biased;

        result = async {
            while let Some(joined) = node_tasks.join_next().await {
                joined.map_err(HandleErr::from).flatten()?;
            }
            Ok(())
        } => result,
        _ = interrupt.recv() => Ok(()),
        _ = terminate.recv() => Ok(()),
    };

    // stop gracefully in every case, a failed task included: the others still answer what they
    // can
    token.cancel();
    let stopped = tokio::time::timeout(shutdown_grace, async {
        while let Some(joined) = node_tasks.join_next().await {
            if let Err(e) = joined.map_err(HandleErr::from).flatten() {
                tracing::warn!(error = %e, "node task failed while stopping");
            }
        }
    })
    .await;
    if stopped.is_err() {
        tracing::warn!(
            ?shutdown_grace,
            "node did not stop in time, aborting its tasks"
        );
    }

    Ok(outcome?)
}

async fn receive_and_dispatch(
    token: CancellationToken,
    command_tx: mpsc::Sender<AppCommand>,
    transport: Transport,
    mut reader: impl MessageReader,
) -> Result<(), HandleErr> {
    loop {
        let msg = tokio::select! {
            biased;

            _ = token.cancelled() => return Ok(()),
            msg = reader.next::<Payload>() => msg,
        };

        match msg {
            Ok(Some(msg)) => dispatch(msg, &command_tx, &transport).await?,
            Ok(None) => return Ok(()),
            Err(IoError::Malformed {
                src,
                msg_id,
                source,
            }) => {
                tracing::warn!(?src, ?msg_id, error = %source, "malformed message");
                if let (Some(dest), Some(in_reply_to)) = (src, msg_id) {
                    let reply_ctx = ReplyContext {
                        dest,
                        in_reply_to,
                        request: None,
                    };
                    let error = CommandError::new(ErrorKind::Protocol, source.to_string());
                    transport.reply_error(error, reply_ctx).await?;
                }
            }
            Err(e) => return Err(e.into()),
        }
    }
}

async fn dispatch(
    msg: Msg<Payload>,
    command_tx: &mpsc::Sender<AppCommand>,
    transport: &Transport,
) -> Result<(), HandleErr> {
    let src = msg.src.clone();
    match msg.classify() {
        Ok(Incoming::AppCommand(command)) => {
            if transport.deduplicate(&command.reply_ctx).await? == Delivery::Duplicate {
                return Ok(());
            }

            match command_tx.try_send(command) {
                Ok(()) => Ok(()),
                Err(TrySendError::Full(command)) => {
                    tracing::warn!(src = %command.reply_ctx.dest, "command queue is full, rejecting command");
                    let error = CommandError::new(ErrorKind::Unavailable, "node is overloaded");
                    Ok(transport.reply_error(error, command.reply_ctx).await?)
                }
                // the app is stopping: the command was not executed
                Err(TrySendError::Closed(command)) => {
                    let error = CommandError::new(ErrorKind::Unavailable, "node is shutting down");
                    Ok(transport.reply_error(error, command.reply_ctx).await?)
                }
            }
        }
        Ok(Incoming::CommandReply(reply)) => {
            tracing::debug!(src = %reply.src, in_reply_to = reply.in_reply_to, "received command reply");
            transport.register_reply(reply.in_reply_to, reply.result);
            Ok(())
        }
        Err(DropReason::NoMsgId) => {
            tracing::warn!(%src, "dropping command without msg_id");
            Ok(())
        }
        Err(DropReason::NoInReplyTo) => {
            tracing::warn!(%src, "dropping reply without in_reply_to");
            Ok(())
        }
        Err(DropReason::ClientReply) => {
            tracing::warn!(%src, "dropping client-protocol reply, the node sends no client requests");
            Ok(())
        }
    }
}

/// Writes messages until every producer is gone, so replies produced while the node stops are
/// still written.
///
/// # Errors
///
/// [`HandleErr::Io`] if a message can't be serialized or written.
async fn write(
    mut writer: impl MessageWriter,
    mut write_rx: mpsc::Receiver<message::Outgoing>,
    node_id: &str,
) -> Result<(), HandleErr> {
    while let Some(msg) = write_rx.recv().await {
        writer.send(msg.into_msg(node_id)).await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use tokio::sync::mpsc;
    use tokio_util::bytes::Bytes;

    use crate::{
        app::{CommandError, ErrorKind},
        maelstrom::protocol::{
            value::{Base64Bytes, JsonValue},
            wire::{Body, ForwardedPayload, Msg, Payload, RequestId},
        },
        message::Outgoing,
        transport::{RpcConfig, Transport},
    };

    use super::dispatch;

    fn msg(src: &str, msg_id: Option<u64>, payload: Payload) -> Msg<Payload> {
        Msg {
            src: src.to_owned(),
            dest: "n1".to_owned(),
            body: Body {
                id: msg_id,
                in_reply_to: None,
                payload,
            },
        }
    }

    fn client_send(msg_id: u64) -> Msg<Payload> {
        msg(
            "c1",
            Some(msg_id),
            Payload::Send {
                key: "k".to_owned(),
                msg: JsonValue(Bytes::from_static(b"7")),
            },
        )
    }

    fn peer_send(msg_id: u64, seq: u64) -> Msg<Payload> {
        msg(
            "n2",
            Some(msg_id),
            Payload::Fwd {
                request_id: RequestId {
                    incarnation: 1,
                    seq,
                },
                watermark: seq,
                payload: ForwardedPayload::Send {
                    key: "k".to_owned(),
                    msg: Base64Bytes(Bytes::from_static(b"7")),
                },
            },
        )
    }

    fn transport() -> (Transport, mpsc::Receiver<Outgoing>) {
        let (write_tx, write_rx) = mpsc::channel(8);
        (Transport::new(write_tx, RpcConfig::default(), 1), write_rx)
    }

    #[tokio::test]
    async fn full_queue_rejects_command_with_unavailable() {
        let (transport, mut write_rx) = transport();
        let (command_tx, _command_rx) = mpsc::channel(1);

        dispatch(client_send(1), &command_tx, &transport)
            .await
            .expect("first command is queued");
        dispatch(client_send(2), &command_tx, &transport)
            .await
            .expect("overload is not a node failure");

        assert!(matches!(
            write_rx.try_recv(),
            Ok(Outgoing::ErrorReply {
                in_reply_to: 2,
                error: CommandError {
                    kind: ErrorKind::Unavailable,
                    ..
                },
                ..
            })
        ));
    }

    #[tokio::test]
    async fn rejected_peer_request_executes_on_retry() {
        let (transport, mut write_rx) = transport();
        let (command_tx, mut command_rx) = mpsc::channel(1);

        dispatch(client_send(1), &command_tx, &transport)
            .await
            .expect("fills the queue");
        dispatch(peer_send(10, 0), &command_tx, &transport)
            .await
            .expect("rejected");
        assert!(matches!(
            write_rx.try_recv(),
            Ok(Outgoing::ErrorReply {
                in_reply_to: 10,
                ..
            })
        ));

        command_rx.recv().await.expect("frees the queue");

        // the retry must not be treated as "in progress": the rejected attempt was not applied
        dispatch(peer_send(11, 0), &command_tx, &transport)
            .await
            .expect("queued");
        let command = command_rx.try_recv().expect("retry reaches the app");
        assert_eq!(command.reply_ctx.in_reply_to, 11);
    }

    #[tokio::test]
    async fn command_without_msg_id_is_dropped() {
        let (transport, mut write_rx) = transport();
        let (command_tx, mut command_rx) = mpsc::channel(1);

        let no_id = msg(
            "c1",
            None,
            Payload::Send {
                key: "k".to_owned(),
                msg: JsonValue(Bytes::from_static(b"7")),
            },
        );
        dispatch(no_id, &command_tx, &transport)
            .await
            .expect("not a node failure");

        assert!(command_rx.try_recv().is_err());
        assert!(write_rx.try_recv().is_err());
    }

    /// Reads the given lines, then reports the end of input.
    struct LinesReader(std::collections::VecDeque<String>);

    impl crate::io::MessageReader for LinesReader {
        async fn next<'a, T: serde::de::DeserializeOwned + 'a>(
            &'a mut self,
        ) -> Result<Option<Msg<T>>, crate::io::IoError> {
            let Some(line) = self.0.pop_front() else {
                return Ok(None);
            };
            serde_json::from_str(&line)
                .map(Some)
                .map_err(|source| crate::io::IoError::Malformed {
                    src: None,
                    msg_id: None,
                    source,
                })
        }
    }

    /// Collects written messages as JSON.
    #[derive(Clone, Default)]
    struct CollectingWriter(std::sync::Arc<std::sync::Mutex<Vec<serde_json::Value>>>);

    impl crate::io::MessageWriter for CollectingWriter {
        async fn send<T: serde::Serialize + Send>(
            &mut self,
            msg: Msg<T>,
        ) -> Result<(), crate::io::IoError> {
            let value = serde_json::to_value(&msg).expect("serializable");
            self.0.lock().expect("not poisoned").push(value);
            Ok(())
        }
    }

    #[tokio::test]
    async fn node_stops_at_end_of_input_after_writing_replies() {
        let reader = LinesReader(
            [
                r#"{"src":"c1","dest":"n1","body":{"type":"send","msg_id":1,"key":"k","msg":7}}"#,
                r#"{"src":"c1","dest":"n1","body":{"type":"poll","msg_id":2,"offsets":{"k":0}}}"#,
            ]
            .map(str::to_owned)
            .into(),
        );
        let writer = CollectingWriter::default();

        let run = super::run(super::Config::default(), "n1", &[], reader, writer.clone());
        tokio::time::timeout(std::time::Duration::from_secs(5), run)
            .await
            .expect("node stops at the end of input")
            .expect("no node error");

        let written = writer.0.lock().expect("not poisoned").clone();
        let types: Vec<_> = written
            .iter()
            .map(|msg| msg["body"]["type"].as_str().unwrap_or_default().to_owned())
            .collect();
        assert_eq!(types.len(), 2, "{written:?}");
        assert!(types.contains(&"send_ok".to_owned()), "{written:?}");
        assert!(types.contains(&"poll_ok".to_owned()), "{written:?}");
    }

    #[tokio::test]
    async fn writer_writes_queued_messages_until_producers_are_gone() {
        let (write_tx, write_rx) = mpsc::channel(4);
        for in_reply_to in 1..=3 {
            write_tx
                .send(Outgoing::ErrorReply {
                    msg_id: in_reply_to,
                    error: CommandError::new(ErrorKind::Unavailable, "x"),
                    in_reply_to,
                    dest: "c1".to_owned(),
                })
                .await
                .expect("queued");
        }
        drop(write_tx);

        let writer = CollectingWriter::default();
        super::write(writer.clone(), write_rx, "n1")
            .await
            .expect("written");

        assert_eq!(writer.0.lock().expect("not poisoned").len(), 3);
    }

    #[tokio::test]
    async fn closed_queue_rejects_command_with_unavailable() {
        let (transport, mut write_rx) = transport();
        let (command_tx, command_rx) = mpsc::channel(1);
        drop(command_rx);

        dispatch(client_send(1), &command_tx, &transport)
            .await
            .expect("not a node failure");

        assert!(matches!(
            write_rx.try_recv(),
            Ok(Outgoing::ErrorReply {
                in_reply_to: 1,
                error: CommandError {
                    kind: ErrorKind::Unavailable,
                    ..
                },
                ..
            })
        ));
    }

    /// Fails every write.
    struct FailingWriter;

    impl crate::io::MessageWriter for FailingWriter {
        async fn send<T: serde::Serialize + Send>(
            &mut self,
            _msg: Msg<T>,
        ) -> Result<(), crate::io::IoError> {
            let error = std::io::Error::other("broken pipe");
            Err(crate::io::stdio::StdioErr::Write(error).into())
        }
    }

    #[tokio::test]
    async fn node_stops_and_reports_a_failed_task() {
        let reader = LinesReader(
            [r#"{"src":"c1","dest":"n1","body":{"type":"send","msg_id":1,"key":"k","msg":7}}"#]
                .map(str::to_owned)
                .into(),
        );

        let run = super::run(super::Config::default(), "n1", &[], reader, FailingWriter);
        let result = tokio::time::timeout(std::time::Duration::from_secs(5), run)
            .await
            .expect("node stops after a task failed");

        assert!(matches!(
            result,
            Err(super::NodeErr::Handle(super::HandleErr::Io(_)))
        ));
    }
}
