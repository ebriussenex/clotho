use crate::{
    app::{App, AppCommand, AppErr, CommitedOffsets, Ownership},
    io::{IoError, MessageReader, MessageWriter},
    logs::{Store, StoreConfig},
    maelstrom::protocol::wire::Payload,
    message::{self, DropReason, Incoming},
    transport::{self, RpcConfig, Transport, TransportErr},
};

use std::sync::Arc;

use thiserror::Error;
use tokio::{
    sync::mpsc::{self},
    task::JoinSet,
};
use tokio_util::sync::CancellationToken;

#[derive(Debug, Error)]
pub enum NodeErr {
    #[error(transparent)]
    Handle(#[from] HandleErr),
    #[error(transparent)]
    Msg(#[from] MsgErr),
}

#[derive(Debug, Error)]
pub enum MsgErr {
    #[error("missing field in incoming message: {0}")]
    MissingField(&'static str),
}

#[derive(Debug, Error)]
pub enum HandleErr {
    #[error("failed to send to {channel} channel")]
    ChanSend { channel: &'static str },
    #[error(transparent)]
    App(#[from] AppErr),
    #[error(transparent)]
    Transport(#[from] TransportErr),
    #[error(transparent)]
    Msg(#[from] MsgErr),
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
    /// Rendezvous hashing seed. Must be identical on every node; will come from the
    /// configuration (env) later.
    ownership_seed: u64,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            poll_max_msgs_size: 100,
            rpc: RpcConfig::default(),
            max_in_flight_commands: 64,
            ownership_seed: 0,
        }
    }
}

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
    let (command_tx, command_rx) = mpsc::channel(32);

    let mut join_set = JoinSet::new();
    let token = CancellationToken::new();
    let transport = Transport::new(write_tx, config.rpc, transport::new_incarnation());
    let logs = Store::new(StoreConfig::default(), token.child_token());
    let commited_offsets = CommitedOffsets::new();
    let ownership = Ownership::new(
        node_id.to_owned(),
        peer_ids.iter().cloned(),
        config.ownership_seed,
    );

    let node_id: Arc<str> = Arc::from(node_id);
    // let replication = Replication::new(logs.clone(), transport.clone(), node_ids.clone());

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
                commited_offsets,
                ownership,
                config.poll_max_msgs_size,
                config.max_in_flight_commands,
            )
            .run(token, command_rx)
            .await
            .map_err(HandleErr::from)
        }
    });

    // join_set.spawn({
    //     let token = token.clone();
    //     let replication = replication.clone();
    //
    //     async move {
    //         replication
    //             .run_inbound(token, replication_rx)
    //             .await
    //             .map_err(HandleErr::from)
    //     }
    // });
    //
    // join_set.spawn({
    //     let replication = replication.clone();
    //     let token = token.clone();
    //     async move {
    //         replication
    //             .run_delta_sender(token, config.replication_delta_interval)
    //             .await
    //             .map_err(HandleErr::from)
    //     }
    // });
    //
    // join_set.spawn({
    //     let replication = replication.clone();
    //     let token = token.clone();
    //     async move {
    //         replication
    //             .run_digest_announcer(token, config.replication_digest_interval)
    //             .await
    //             .map_err(HandleErr::from)
    //     }
    // });

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

        match msg? {
            Some(msg) => match msg.classify() {
                Ok(msg) => match msg {
                    Incoming::AppCommand(command) => {
                        // retries of peer requests are answered or dropped by transport
                        if transport.admit(&command.reply_ctx).await? {
                            command_tx
                                .send(command)
                                .await
                                .map_err(|_| HandleErr::chan_send::<AppCommand>("command_tx"))?;
                        }
                    }
                    Incoming::CommandReply(reply) => {
                        tracing::debug!(src = %reply.src, in_reply_to = reply.in_reply_to, "received command reply");
                        transport.register_reply(reply.in_reply_to, reply.result);
                    }
                },
                Err(reason) => match reason {
                    DropReason::NoMsgId => Err(HandleErr::Msg(MsgErr::MissingField("msg_id")))?,
                    DropReason::NoInReplyTo => {
                        Err(HandleErr::Msg(MsgErr::MissingField("in_reply_to")))?
                    }
                },
            },
            None => return Ok(()),
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
