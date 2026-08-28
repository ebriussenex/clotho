use crate::{
    app::{App, AppCommand, AppErr, CommitedOffsets},
    io::{IoError, MessageReader, MessageWriter},
    kv::KvEvent,
    logs::Logs,
    maelstrom::protocol::wire::Payload,
    message::{self, DropReason, Incoming},
    replication::{Replication, ReplicationCommand, ReplicationErr},
    transport::{Transport, TransportErr},
};

use std::{sync::Arc, time::Duration};

use backon::ExponentialBuilder;
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
    Replication(#[from] ReplicationErr),
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
    request_timeout: Duration,
    transport_backoff: ExponentialBuilder,
    cas_backoff: ExponentialBuilder,
    replication_delta_interval: Duration,
    replication_digest_interval: Duration,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            poll_max_msgs_size: 100,
            request_timeout: Duration::from_millis(1000),
            transport_backoff: ExponentialBuilder::default()
                .with_jitter()
                .with_max_times(3)
                .with_min_delay(Duration::from_millis(25))
                .with_max_delay(Duration::from_millis(250)),
            cas_backoff: ExponentialBuilder::default()
                .with_jitter()
                .with_max_times(8)
                .with_min_delay(Duration::from_millis(5))
                .with_max_delay(Duration::from_millis(100)),
            replication_delta_interval: Duration::from_millis(1000),
            replication_digest_interval: Duration::from_millis(1000),
        }
    }
}

pub async fn run<R, W>(
    config: Config,
    node_id: String,
    node_ids: Vec<String>,
    reader: R,
    writer: W,
) -> Result<(), NodeErr>
where
    R: MessageReader,
    W: MessageWriter,
{
    let (write_tx, write_rx) = mpsc::channel(32);
    let (client_tx, client_rx) = mpsc::channel(32);
    let (kv_tx, kv_rx) = mpsc::channel(32);
    let (replication_tx, replication_rx) = mpsc::channel(32);

    let mut join_set = JoinSet::new();
    let token = CancellationToken::new();
    let transport = Transport::new(write_tx, config.request_timeout, config.transport_backoff);
    let logs = Logs::<u32>::default();
    let commited_offsets = CommitedOffsets::new();
    let node_id = Arc::new(node_id);
    let node_ids = Arc::new(node_ids);
    let replication = Replication::new(logs.clone(), transport.clone(), node_ids.clone());

    join_set.spawn({
        let token = token.clone();
        async move { receive_and_dispatch(token, client_tx, kv_tx, replication_tx, reader).await }
    });

    join_set.spawn({
        let token = token.clone();
        let node_id = node_id.clone();
        async move { write(token, writer, write_rx, node_id).await }
    });

    join_set.spawn({
        let token = token.clone();
        let transport = transport.clone();
        let logs = logs.clone();
        let commited_offsets = commited_offsets.clone();

        async move {
            App::new(
                logs,
                transport,
                commited_offsets,
                config.poll_max_msgs_size,
                config.cas_backoff,
            )
            .run(token, client_rx)
            .await
            .map_err(HandleErr::from)
        }
    });

    join_set.spawn({
        let token = token.clone();
        let transport = transport.clone();

        async move { process_kv_events(token, kv_rx, transport).await }
    });

    join_set.spawn({
        let token = token.clone();
        let replication = replication.clone();

        async move {
            replication
                .run_inbound(token, replication_rx)
                .await
                .map_err(HandleErr::from)
        }
    });

    join_set.spawn({
        let replication = replication.clone();
        let token = token.clone();
        async move {
            replication
                .run_delta_sender(token, config.replication_delta_interval)
                .await
                .map_err(HandleErr::from)
        }
    });

    join_set.spawn({
        let replication = replication.clone();
        let token = token.clone();
        async move {
            replication
                .run_digest_announcer(token, config.replication_digest_interval)
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
    client_tx: mpsc::Sender<AppCommand>,
    kv_tx: mpsc::Sender<KvEvent>,
    replication_tx: mpsc::Sender<ReplicationCommand>,
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
                    Incoming::AppCommand(command) => client_tx
                        .send(command)
                        .await
                        .map_err(|_| HandleErr::chan_send::<AppCommand>("client_tx"))?,
                    Incoming::KvEvent(kv_event) => kv_tx
                        .send(kv_event)
                        .await
                        .map_err(|_| HandleErr::chan_send::<KvEvent>("kv_tx"))?,
                    Incoming::ReplicationCommand(command) => {
                        replication_tx.send(command).await.map_err(|_| {
                            HandleErr::chan_send::<ReplicationCommand>("replication_tx")
                        })?
                    }
                },
                Err(reason) => match reason {
                    DropReason::NoMsgId => Err(HandleErr::Msg(MsgErr::MissingField("msg_id")))?,
                    DropReason::NoInReplyTo => {
                        Err(HandleErr::Msg(MsgErr::MissingField("in_reply_to")))?
                    }
                    DropReason::Ignored => (),
                },
            },
            None => return Ok(()),
        }
    }
}

async fn process_kv_events(
    token: CancellationToken,
    mut kv_rx: mpsc::Receiver<KvEvent>,
    transport: Transport,
) -> Result<(), HandleErr> {
    loop {
        let event = tokio::select! {
            biased;

            _ = token.cancelled() => return Ok(()),
            event = kv_rx.recv() => event,
        };

        match event {
            Some(event) => transport.register_kv_event(event).await?,
            None => return Ok(()),
        }
    }
}

async fn write(
    token: CancellationToken,
    mut writer: impl MessageWriter,
    mut write_rx: mpsc::Receiver<message::Outgoing>,
    node_id: Arc<String>,
) -> Result<(), HandleErr> {
    loop {
        let msg = tokio::select! {
            biased;

            _ = token.cancelled() => return Ok(()),
            msg = write_rx.recv() => msg,
        };

        match msg {
            Some(msg) => writer.send(msg.into_msg(node_id.as_str())).await?,
            None => return Ok(()),
        }
    }
}
