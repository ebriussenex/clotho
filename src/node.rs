use crate::{
    io::{IoError, MessageReader, MessageWriter},
    message::{
        self, CasReq, CasResUnknown, ClientRequest, ClientResponse, CommitReq, DropReason,
        GossipDiffReq, GossipDigestReq, Incoming, InterNodeReq, KvEvent, ListCommittedOffsetsOkRes,
        ListReq, PollOkRes, PollReq, ReadReq, SendOkRes, SendReq,
    },
    storage::{Logs, StorageErr},
    transport::{Transport, TransportErr},
};

use std::{
    collections::{HashMap, hash_map::Entry},
    sync::Arc,
};

use thiserror::Error;
use tokio::{
    sync::mpsc::{self},
    task::JoinSet,
    time::Duration,
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
    #[error("failed to send to {0} channel")]
    ChanSend(&'static str),
    #[error("unexpected cas result: code: {code} text: {text}")]
    UnexpectedCas { code: u8, text: String },
    #[error(transparent)]
    Storage(#[from] StorageErr),
    #[error(transparent)]
    Transport(#[from] TransportErr),
    #[error(transparent)]
    Msg(#[from] MsgErr),
    #[error(transparent)]
    Io(#[from] IoError),
}

pub struct Config {
    poll_max_msgs_size: usize,
    request_timeout: Duration,
    gossip_diff_interval: Duration,
    gossip_digest_interval: Duration,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            poll_max_msgs_size: 100,
            request_timeout: Duration::from_millis(1000),
            gossip_diff_interval: Duration::from_millis(1000),
            gossip_digest_interval: Duration::from_millis(1000),
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
    let (internode_tx, internode_rx) = mpsc::channel(32);

    let mut join_set = JoinSet::new();
    let token = CancellationToken::new();
    let transport = Transport::new(write_tx, config.request_timeout);
    let logs = Logs::new();
    let commited_offsets = Arc::new(tokio::sync::RwLock::new(HashMap::new()));
    let observed_offsets = Arc::new(tokio::sync::RwLock::new(HashMap::new()));
    let node_id: Arc<str> = node_id.into();
    let node_ids = Arc::new(node_ids);

    join_set.spawn({
        let token = token.clone();
        async move { receive_and_dispatch(token, client_tx, kv_tx, internode_tx, reader).await }
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
            process_client_requests(
                token,
                client_rx,
                logs,
                transport,
                commited_offsets,
                config.poll_max_msgs_size,
            )
            .await
        }
    });

    join_set.spawn({
        let token = token.clone();
        let transport = transport.clone();

        async move { process_kv_events(token, kv_rx, transport).await }
    });

    join_set.spawn({
        let token = token.clone();
        let logs = logs.clone();
        let observed_offsets = observed_offsets.clone();

        async move { process_internode(token, internode_rx, logs, observed_offsets).await }
    });

    join_set.spawn({
        let transport = transport.clone();
        let observed_offsets = observed_offsets.clone();
        let logs = logs.clone();
        let node_ids = node_ids.clone();
        let token = token.clone();
        async move {
            gossip_diff(
                token,
                config.gossip_diff_interval,
                observed_offsets,
                logs,
                transport,
                node_ids,
            )
            .await
        }
    });

    join_set.spawn({
        let transport = transport.clone();
        let logs = logs.clone();
        let node_ids = node_ids.clone();
        let token = token.clone();
        let node_id = node_id.clone();

        async move {
            gossip_digest(
                token,
                config.gossip_digest_interval,
                logs,
                transport,
                node_ids,
                node_id,
            )
            .await
        }
    });

    tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            token.cancel();
        }
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
    }

    token.cancel();
    while join_set.join_next().await.is_some() {}

    Ok(())
}

async fn receive_and_dispatch(
    token: CancellationToken,
    client_tx: mpsc::Sender<ClientRequest>,
    kv_tx: mpsc::Sender<KvEvent>,
    internode_tx: mpsc::Sender<InterNodeReq>,
    mut reader: impl MessageReader,
) -> Result<(), HandleErr> {
    loop {
        let msg = tokio::select! {
            _ = token.cancelled() => return Ok(()),
            msg = reader.next() => msg,
        };

        match msg? {
            Some(msg) => match msg.classify() {
                Ok(msg) => match msg {
                    Incoming::ClientRequest(client_request) => client_tx
                        .send(client_request)
                        .await
                        .map_err(|_| HandleErr::ChanSend("client_tx"))?,
                    Incoming::KvEvent(kv_event) => kv_tx
                        .send(kv_event)
                        .await
                        .map_err(|_| HandleErr::ChanSend("kv_event"))?,
                    Incoming::NodeRequest(node_req) => internode_tx
                        .send(node_req)
                        .await
                        .map_err(|_| HandleErr::ChanSend("node_req"))?,
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

async fn process_client_requests(
    token: CancellationToken,
    mut client_rx: mpsc::Receiver<ClientRequest>,
    logs: Logs,
    transport: Transport,
    commited_offsets: Arc<tokio::sync::RwLock<HashMap<String, usize>>>,
    poll_max_msgs_size: usize,
) -> Result<(), HandleErr> {
    loop {
        let req = tokio::select! {
            _ = token.cancelled() => return Ok(()),
            req = client_rx.recv() => req,
        };

        match req {
            Some(req) => match req {
                ClientRequest::Send(req) => {
                    handle_send(logs.clone(), req, transport.clone()).await?
                }
                ClientRequest::Poll(req) => {
                    handle_poll(logs.clone(), req, transport.clone(), poll_max_msgs_size).await?
                }
                ClientRequest::CommitOffsets(req) => {
                    handle_commit_offsets(commited_offsets.clone(), transport.clone(), req).await?
                }
                ClientRequest::ListCommittedOffsets(req) => {
                    handle_list_commited_offsets(commited_offsets.clone(), transport.clone(), req)
                        .await?
                }
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
    node_id: Arc<str>,
) -> Result<(), HandleErr> {
    loop {
        let msg = tokio::select! {
            _ = token.cancelled() => return Ok(()),
            msg = write_rx.recv() => msg,
        };

        match msg {
            Some(msg) => writer.send(msg.into_msg(&node_id)).await?,
            None => return Ok(()),
        }
    }
}

async fn handle_poll(
    logs: Logs,
    req: PollReq,
    transport: Transport,
    limit: usize,
) -> Result<(), TransportErr> {
    let resp = ClientResponse::PollOk(PollOkRes {
        msgs: logs.poll(req.offsets, limit).await,
    });
    transport
        .send_client_response(resp, req.msg_id, req.src)
        .await
}

async fn handle_list_commited_offsets(
    commited_offsets: Arc<tokio::sync::RwLock<HashMap<String, usize>>>,
    transport: Transport,
    req: ListReq,
) -> Result<(), TransportErr> {
    let resp = ClientResponse::ListCommittedOffsetsOk(ListCommittedOffsetsOkRes {
        offsets: commited_offsets.read().await.clone(),
    });
    transport
        .send_client_response(resp, req.msg_id, req.src)
        .await
}

async fn handle_send(logs: Logs, req: SendReq, transport: Transport) -> Result<(), HandleErr> {
    let key = req.key;
    let next_offset = logs.next_offset(&key).await;

    let cas_req = CasReq {
        key: key.clone(),
        from: next_offset,
        to: next_offset + 1,
    };

    let offset = rpc_cas(cas_req, transport.clone()).await?;

    logs.append(key, req.msg, offset).await?;

    let res = ClientResponse::SendOk(SendOkRes { offset });

    transport
        .send_client_response(res, req.msg_id, req.src)
        .await?;
    Ok(())
}

async fn handle_commit_offsets(
    commited_offsets: Arc<tokio::sync::RwLock<HashMap<String, usize>>>,
    transport: Transport,
    req: CommitReq,
) -> Result<(), TransportErr> {
    let mut com_off = commited_offsets.write().await;

    req.offsets.iter().for_each(|(key, noff)| {
        com_off
            .entry(key.clone())
            .and_modify(|off| *off = *noff)
            .or_insert(*noff);
    });

    transport
        .send_client_response(ClientResponse::CommitOffsetsOk, req.msg_id, req.src)
        .await
}

async fn rpc_cas(mut cas_req: CasReq, transport: Transport) -> Result<usize, HandleErr> {
    loop {
        let offset = cas_req.from;
        let key = cas_req.key.clone();
        let cas_res = transport.rpc_cas(&cas_req).await?;

        match cas_res {
            message::CasRes::Ok(_) => return Ok(offset),
            message::CasRes::Retry(_) => {
                let latest_offset = transport.rpc_read(&ReadReq { key }).await?;

                cas_req.from = latest_offset;
                cas_req.to = latest_offset + 1;

                continue;
            }
            message::CasRes::Unknown(CasResUnknown { code, text, .. }) => {
                return Err(HandleErr::UnexpectedCas { code, text });
            }
        }
    }
}

async fn process_internode(
    token: CancellationToken,
    mut internode_rx: mpsc::Receiver<InterNodeReq>,
    mut logs: Logs,
    observed_offsets: Arc<tokio::sync::RwLock<HashMap<String, HashMap<String, usize>>>>,
) -> Result<(), HandleErr> {
    loop {
        let req = tokio::select! {
            _ = token.cancelled() => return Ok(()),
            req = internode_rx.recv() => req,
        };

        match req {
            Some(req) => handle_internode_req(req, &mut logs, &observed_offsets).await?,
            None => return Ok(()),
        }
    }
}

async fn handle_internode_req(
    req: InterNodeReq,
    logs: &mut Logs,
    observed_offsets: &Arc<tokio::sync::RwLock<HashMap<String, HashMap<String, usize>>>>,
) -> Result<(), HandleErr> {
    match req {
        InterNodeReq::GossipDigest(digest_req) => {
            handle_digest(observed_offsets.clone(), digest_req).await
        }
        InterNodeReq::GossipDiff(diff_req) => handle_diff(logs, diff_req).await,
    }
}

async fn handle_digest(
    observed_offsets: Arc<tokio::sync::RwLock<HashMap<String, HashMap<String, usize>>>>,
    req: GossipDigestReq,
) -> Result<(), HandleErr> {
    let GossipDigestReq {
        src,
        digest: offsets,
    } = req;

    match observed_offsets.write().await.entry(src) {
        Entry::Occupied(mut e) => *e.get_mut() = offsets,
        Entry::Vacant(e) => {
            let _ = e.insert(offsets);
        }
    }
    Ok(())
}

async fn handle_diff(logs: &mut Logs, req: GossipDiffReq) -> Result<(), HandleErr> {
    logs.merge(req.msgs).await.map_err(HandleErr::Storage)
}

async fn gossip_diff(
    token: CancellationToken,
    interval: Duration,
    observed_offsets: Arc<tokio::sync::RwLock<HashMap<String, HashMap<String, usize>>>>,
    logs: Logs,
    transport: Transport,
    node_ids: Arc<Vec<String>>,
) -> Result<(), HandleErr> {
    let mut interval = tokio::time::interval(interval);
    loop {
        tokio::select! {
            _ = token.cancelled() => return Ok(()),
            _ = interval.tick() => {},
        }

        let observed_offsets = observed_offsets.read().await;
        for dest in node_ids.iter() {
            let Some(observed_offset) = observed_offsets.get(dest) else {
                continue;
            };

            let diff = logs.diff(observed_offset.clone()).await;
            let req = InterNodeReq::GossipDiff(GossipDiffReq { msgs: diff });
            transport
                .send_internode_req(dest, req)
                .await
                .map_err(HandleErr::Transport)?;
        }
    }
}

async fn gossip_digest(
    token: CancellationToken,
    interval: Duration,
    logs: Logs,
    transport: Transport,
    node_ids: Arc<Vec<String>>,
    src: Arc<str>,
) -> Result<(), HandleErr> {
    let mut interval = tokio::time::interval(interval);
    loop {
        tokio::select! {
            _ = token.cancelled() => return Ok(()),
            _ = interval.tick() => {},
        }

        for dest in node_ids.iter() {
            let req = InterNodeReq::GossipDigest(GossipDigestReq {
                digest: logs.digest().await,
                src: src.to_string(),
            });
            transport
                .send_internode_req(dest, req)
                .await
                .map_err(HandleErr::Transport)?;
        }
    }
}
