use crate::{
    message::{
        self, Body, CasReq, CasResUnknown, ClientRequest, ClientResponse, CommitReq, DropReason,
        Incoming, KvEvent, ListCommittedOffsetsOkRes, ListReq, Msg, Payload, PollOkRes, PollReq,
        ReadReq, SendReq,
    },
    storage::{Logs, StorageErr},
    transport::{Transport, TransportErr},
};

use std::{collections::HashMap, io::Write, sync::Arc};

use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::{
    io::{AsyncBufReadExt, BufReader, Lines},
    sync::mpsc::{self},
};

#[derive(Debug, Error)]
pub enum NodeErr {
    #[error("failed to read init msg: {0}")]
    ReadInit(#[source] std::io::Error),
    #[error("no init message")]
    NoInitMsg,
    #[error("failed to deserialize init msg: {0}")]
    InitMsgDeserialize(#[source] serde_json::Error),
    #[error("failed to write to stdout: {0}")]
    Write(#[source] std::io::Error),
    #[error("failed to read line from stdin")]
    ReadStdIn(#[source] std::io::Error),
    #[error("failed to serialize msg: {0}")]
    Serialize(#[source] serde_json::Error),
    #[error("failed to deserialize msg: {0}")]
    Deserialize(#[source] serde_json::Error),
    #[error{"no \"id\" in {0} msg"}]
    NoId(String),
    #[error("failed to send to {0} channel")]
    ChanSend(&'static str),

    #[error(transparent)]
    Handle(#[from] HandleErr),
    #[error(transparent)]
    Msg(#[from] MsgErr),
}

#[derive(Debug, Error)]
pub enum MsgErr {
    #[error("missing field in incoming message: {0}")]
    MissingField(&'static str),
    #[error("no \"in_reply_to\" in {0} msg")]
    NoReply(&'static str),
}

#[derive(Debug, Error)]
pub enum HandleErr {
    #[error("not found log by key")]
    KeyNotFound,

    #[error("failed to send to {0} channel")]
    ChanSend(&'static str),
    #[error("unexpected cas result: code: {code} text: {text}")]
    UnexpectedCas { code: u8, text: String },
    #[error(transparent)]
    Storage(#[from] StorageErr),
    #[error(transparent)]
    Transport(#[from] TransportErr),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
pub enum InitPayload {
    Init(Init),
    InitOk,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Init {
    node_id: String,
    node_ids: Vec<String>,
}

pub struct Node<W: Write + Send + 'static> {
    node_id: String,
    node_ids: Vec<String>,
    writer: Arc<tokio::sync::Mutex<W>>,
    commited_offsets: Arc<tokio::sync::RwLock<HashMap<String, usize>>>,
    config: Config,
}

pub struct Config {
    poll_max_msgs_size: usize,
    request_timeout: tokio::time::Duration,
}

impl<W: Write + Send + 'static> Node<W> {
    pub fn new(writer: Arc<tokio::sync::Mutex<W>>, init_msg: Msg<Init>, config: Config) -> Self {
        let node_id = init_msg.body.payload.node_id;
        let node_ids: Vec<_> = init_msg
            .body
            .payload
            .node_ids
            .into_iter()
            .filter(|n_id| **n_id != node_id)
            .collect();

        Self {
            node_id,
            writer,
            config,
            node_ids,
            commited_offsets: Arc::new(tokio::sync::RwLock::new(HashMap::new())),
        }
    }

    async fn run(self, lines: Lines<BufReader<tokio::io::Stdin>>) -> Result<(), NodeErr> {
        let commited_offsets: Arc<tokio::sync::RwLock<HashMap<String, usize>>> =
            Arc::new(tokio::sync::RwLock::new(HashMap::new()));

        let (write_tx, write_rx) = mpsc::channel::<message::Outgoing>(32);
        let (client_tx, client_rx) = mpsc::channel(32);
        let (kv_tx, kv_rx) = mpsc::channel(32);

        let dispatch_hndl =
            tokio::spawn(async move { dispatch_input(client_tx, kv_tx, lines).await });

        let writer = Arc::clone(&self.writer);
        let writer_hndl =
            tokio::spawn(async move { process_write(writer, write_rx, &self.node_id).await });

        let transport = Transport::new(write_tx, self.config.request_timeout);
        let logs = Logs::new();

        let client_hndl = tokio::spawn(async move {
            process_client_requests(
                client_rx,
                logs.clone(),
                transport.clone(),
                commited_offsets.clone(),
                self.config.poll_max_msgs_size,
            )
            .await
        });

        Ok(())
    }
}

pub async fn process(config: Config) -> Result<(), NodeErr> {
    let mut in_lines = BufReader::new(tokio::io::stdin()).lines();
    let stdout = Arc::new(tokio::sync::Mutex::new(std::io::stdout()));

    let init_msg_str = &in_lines
        .next_line()
        .await
        .map_err(NodeErr::ReadInit)?
        .ok_or(NodeErr::NoInitMsg)?;

    let init_msg: Msg<Init> =
        serde_json::from_str(init_msg_str).map_err(NodeErr::InitMsgDeserialize)?;

    let init_reply = Msg {
        src: init_msg.dest.clone(),
        dest: init_msg.src.clone(),
        body: Body {
            id: Some(0),
            in_reply_to: init_msg.body.id,
            payload: InitPayload::InitOk,
        },
    };

    //let _ = init_reply
    //    .write(Arc::clone(&stdout))
    //    .inspect_err(|e| eprintln!("failed to write init repy: {e}"));

    let node = Node::new(stdout, init_msg, config);

    node.run(in_lines).await?;

    Ok(())
}

async fn dispatch_input(
    client_tx: mpsc::Sender<ClientRequest>,
    kv_tx: mpsc::Sender<KvEvent>,
    mut lines: Lines<BufReader<tokio::io::Stdin>>,
) -> Result<(), NodeErr> {
    loop {
        match lines.next_line().await.map_err(NodeErr::ReadStdIn)? {
            Some(line) => {
                let msg: Msg<Payload> =
                    serde_json::from_str(&line).map_err(NodeErr::Deserialize)?;

                match msg.classify() {
                    Ok(msg) => match msg {
                        Incoming::ClientRequest(client_request) => client_tx
                            .send(client_request)
                            .await
                            .map_err(|_| NodeErr::ChanSend("client_tx"))?,
                        Incoming::KvEvent(kv_event) => kv_tx
                            .send(kv_event)
                            .await
                            .map_err(|_| NodeErr::ChanSend("kv_event"))?,
                    },
                    Err(reason) => match reason {
                        DropReason::NoMsgId => Err(NodeErr::Msg(MsgErr::MissingField("msg_id")))?,
                        DropReason::NoInReplyTo => {
                            Err(NodeErr::Msg(MsgErr::MissingField("in_reply_to")))?
                        }
                        DropReason::Ignored => {}
                    },
                };
            }
            None => break Ok(()),
        }
    }
}

async fn process_client_requests(
    mut client_rx: mpsc::Receiver<ClientRequest>,
    logs: Logs,
    transport: Transport,
    commited_offsets: Arc<tokio::sync::RwLock<HashMap<String, usize>>>,
    poll_max_msgs_size: usize,
) -> Result<(), HandleErr> {
    while let Some(req) = client_rx.recv().await {
        match req {
            ClientRequest::Send(req) => handle_send(logs.clone(), req, transport.clone()).await?,
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
        }
    }
    Ok(())
}

async fn process_kv_events(
    mut kv_rx: mpsc::Receiver<KvEvent>,
    transport: Transport,
) -> Result<(), HandleErr> {
    while let Some(event) = kv_rx.recv().await {
        transport.register_kv_event(event).await?;
    }
    Ok(())
}

async fn process_write<W: Write + Send + 'static>(
    writer: Arc<tokio::sync::Mutex<W>>,
    mut write_rx: mpsc::Receiver<message::Outgoing>,
    node_id: &str,
) -> Result<(), NodeErr> {
    while let Some(msg) = write_rx.recv().await {
        let msg = msg.into_msg(node_id);
        let mut writer = writer.lock().await;
        let str_msg = serde_json::to_string(&msg).map_err(NodeErr::Serialize)?;

        writer
            .write_all(str_msg.as_bytes())
            .map_err(NodeErr::Write)?;
        writer.write_all(b"\n").map_err(NodeErr::Write)?;
        writer.flush().map_err(NodeErr::Write)?;
    }
    Ok(())
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

async fn handle_send(mut logs: Logs, req: SendReq, transport: Transport) -> Result<(), HandleErr> {
    let key = req.key;
    let offset = logs.last_offset(&key).await.ok_or(HandleErr::KeyNotFound)?;

    let cas_req = CasReq {
        key: key.clone(),
        from: offset,
        to: offset + 1,
    };

    let offset = rpc_cas(cas_req, transport).await?;

    logs.append(key, req.msg, offset)
        .await
        .map_err(|e| HandleErr::Storage(e))?;
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
        let next_offset = cas_req.to;
        let key = cas_req.key.clone();
        let cas_res = transport.rpc_cas(&cas_req).await?;

        match cas_res {
            message::CasRes::Ok(_) => return Ok(next_offset),
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
