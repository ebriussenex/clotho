use crate::{
    app::{App, AppCommand, AppErr, CommandError, ErrorKind, Ownership, ReplyContext},
    io::{IoError, MessageReader, MessageWriter},
    logs::{Store, StoreConfig},
    maelstrom::protocol::wire::{Msg, Payload},
    message::{self, DropReason, Incoming},
    offsets::CommittedOffsets,
    transport::{self, RpcConfig, Transport, TransportErr},
};

use std::sync::Arc;

use thiserror::Error;
use tokio::{
    sync::mpsc::{self, error::TrySendError},
    task::JoinSet,
};
use tokio_util::sync::CancellationToken;

/// Error that stopped the node.
#[derive(Debug, Error)]
pub enum NodeErr {
    /// One of the node's tasks failed.
    #[error(transparent)]
    Handle(#[from] HandleErr),
}

/// Failure of one of the node's tasks (reader, writer, app). Stops the whole node.
#[derive(Debug, Error)]
pub enum HandleErr {
    /// The app stopped taking commands.
    #[error("failed to send to {channel} channel")]
    ChanSend { channel: &'static str },
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

impl HandleErr {
    fn chan_send<T>(channel: &'static str) -> Self {
        tracing::error!(
            channel,
            payload_type = std::any::type_name::<T>(),
            "channel send failed"
        );
        Self::ChanSend { channel }
    }
}

pub struct Config {
    poll_max_msgs_size: usize,
    rpc: RpcConfig,
    max_in_flight_commands: usize,
    command_queue_capacity: usize,
    ownership_seed: u64,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            poll_max_msgs_size: 100,
            rpc: RpcConfig::default(),
            max_in_flight_commands: 64,
            command_queue_capacity: 32,
            ownership_seed: 0,
        }
    }
}

/// Runs the node until the input ends, a task fails, or Ctrl-C.
///
/// # Errors
///
/// The first [`HandleErr`] of the reader, writer, or app task. The other tasks are aborted.
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
    let (write_tx, write_rx) = mpsc::channel(32);
    let (command_tx, command_rx) = mpsc::channel(config.command_queue_capacity.max(1));

    let mut join_set = JoinSet::new();
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

    join_set.spawn({
        let token = token.clone();
        let transport = transport.clone();
        async move { receive_and_dispatch(token, command_tx, transport, reader).await }
    });

    join_set.spawn({
        let token = token.clone();
        async move { write(token, writer, write_rx, node_id.as_ref()).await }
    });

    join_set.spawn({
        let token = token.clone();
        let transport = transport.clone();

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

    tokio::select! {
        biased;

        result = async {
            while let Some(res) = join_set.join_next().await {
                if let Ok(Err(e)) = res {
                    return Err(e);
                }
            }
            Ok(())
        } => {
            result?;
        }
        _ = tokio::signal::ctrl_c() => {
            token.cancel();
        }
    }

    token.cancel();
    while join_set.join_next().await.is_some() {}

    Ok(())
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
            if !transport.admit(&command.reply_ctx).await? {
                return Ok(());
            }

            match command_tx.try_send(command) {
                Ok(()) => Ok(()),
                Err(TrySendError::Full(command)) => {
                    tracing::warn!(src = %command.reply_ctx.dest, "command queue is full, rejecting command");
                    let error = CommandError::new(ErrorKind::Unavailable, "node is overloaded");
                    Ok(transport.reply_error(error, command.reply_ctx).await?)
                }
                Err(TrySendError::Closed(_)) => {
                    Err(HandleErr::chan_send::<AppCommand>("command_tx"))
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

async fn write(
    token: CancellationToken,
    mut writer: impl MessageWriter,
    mut write_rx: mpsc::Receiver<message::Outgoing>,
    node_id: &str,
) -> Result<(), HandleErr> {
    loop {
        let msg = tokio::select! {
            biased;

            _ = token.cancelled() => return Ok(()),
            msg = write_rx.recv() => msg,
        };

        match msg {
            Some(msg) => writer.send(msg.into_msg(node_id.as_ref())).await?,
            None => return Ok(()),
        }
    }
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
}
