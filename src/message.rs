use std::collections::HashMap;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Msg<Payload> {
    pub src: String,
    pub dest: String,
    pub body: Body<Payload>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Body<Payload> {
    #[serde(rename = "msg_id")]
    pub id: Option<usize>,
    pub in_reply_to: Option<usize>,
    #[serde(flatten)]
    pub payload: Payload,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
pub enum Payload {
    Send {
        key: String,
        msg: u32,
    },
    SendOk {
        offset: usize,
    },
    Poll {
        offsets: HashMap<String, usize>,
    },
    PollOk {
        msgs: HashMap<String, Vec<(usize, u32)>>,
    },
    CommitOffsets {
        offsets: HashMap<String, usize>,
    },
    CommitOffsetsOk,
    ListCommittedOffsets {
        keys: Vec<String>,
    },
    ListCommittedOffsetsOk {
        offsets: HashMap<String, usize>,
    },
    Cas {
        key: String,
        from: usize,
        to: usize,
        create_if_not_exists: bool,
    },
    Read {
        key: String,
    },
    ReadOk {
        value: usize,
    },
    CasOk,
    Error {
        code: u8,
        text: String,
    },
    GossipDigest {
        digest: HashMap<String, usize>,
    },
    GossipDiff {
        msgs: HashMap<String, Vec<(usize, Vec<u32>)>>,
    },
}

pub enum InterNodeReq {
    GossipDigest(GossipDigestReq),
    GossipDiff(GossipDiffReq),
}

pub struct GossipDigestReq {
    pub src: String,
    pub digest: HashMap<String, usize>,
}

pub struct GossipDiffReq {
    pub msgs: HashMap<String, Vec<(usize, Vec<u32>)>>,
}

pub enum ClientRequest {
    Send(SendReq),
    Poll(PollReq),
    CommitOffsets(CommitReq),
    ListCommittedOffsets(ListReq),
}

pub struct SendReq {
    pub key: String,
    pub msg: u32,
    pub msg_id: usize,
    pub src: String,
}

pub struct PollReq {
    pub offsets: HashMap<String, usize>,
    pub msg_id: usize,
    pub src: String,
}

pub struct CommitReq {
    pub offsets: HashMap<String, usize>,
    pub msg_id: usize,
    pub src: String,
}

pub struct ListReq {
    pub keys: Vec<String>,
    pub msg_id: usize,
    pub src: String,
}

pub enum KvEvent {
    CasRes(CasRes),
    ReadRes(ReadRes),
}

pub enum CasRes {
    Ok(CasOkRes),
    Retry(CasRetryRes),
    Unknown(CasResUnknown),
}

pub struct CasOkRes {
    pub in_reply_to: usize,
}

pub struct CasRetryRes {
    pub in_reply_to: usize,
}

pub struct ReadRes {
    pub value: usize,
    pub in_reply_to: usize,
}

#[derive(Debug)]
pub struct CasResUnknown {
    pub code: u8,
    pub text: String,
    pub in_reply_to: usize,
}

impl KvEvent {
    pub fn get_in_reply_to(&self) -> usize {
        match self {
            KvEvent::CasRes(cas_res) => match cas_res {
                CasRes::Ok(CasOkRes { in_reply_to }) => *in_reply_to,
                CasRes::Retry(CasRetryRes { in_reply_to }) => *in_reply_to,
                CasRes::Unknown(CasResUnknown { in_reply_to, .. }) => *in_reply_to,
            },
            KvEvent::ReadRes(ReadRes { in_reply_to, .. }) => *in_reply_to,
        }
    }
}

pub enum Incoming {
    ClientRequest(ClientRequest),
    KvEvent(KvEvent),
    NodeRequest(InterNodeReq),
}

pub enum Outgoing {
    KvRequest {
        req: KvRequest,
        msg_id: usize,
    },
    ClientResponse {
        msg_id: usize,
        res: ClientResponse,
        in_reply_to: usize,
        dest: String,
    },
    InterNodeReq {
        req: InterNodeReq,
        msg_id: usize,
        dest: String,
    },
}

pub struct SendOkRes {
    pub offset: usize,
}

pub struct PollOkRes {
    pub msgs: HashMap<String, Vec<(usize, u32)>>,
}

pub struct ListCommittedOffsetsOkRes {
    pub offsets: HashMap<String, usize>,
}

pub enum ClientResponse {
    SendOk(SendOkRes),
    PollOk(PollOkRes),
    CommitOffsetsOk,
    ListCommittedOffsetsOk(ListCommittedOffsetsOkRes),
}

pub enum KvRequest {
    Cas(CasReq),
    Read(ReadReq),
}

#[derive(Clone)]
pub struct CasReq {
    pub key: String,
    pub from: usize,
    pub to: usize,
}

#[derive(Clone)]
pub struct ReadReq {
    pub key: String,
}

impl From<KvRequest> for Payload {
    fn from(req: KvRequest) -> Self {
        match req {
            KvRequest::Cas(CasReq { key, from, to, .. }) => Payload::Cas {
                key,
                from,
                to,
                create_if_not_exists: true,
            },

            KvRequest::Read(ReadReq { key, .. }) => Payload::Read { key },
        }
    }
}

impl From<ClientResponse> for Payload {
    fn from(res: ClientResponse) -> Self {
        match res {
            ClientResponse::SendOk(SendOkRes { offset }) => Payload::SendOk { offset },
            ClientResponse::PollOk(PollOkRes { msgs }) => Payload::PollOk { msgs },
            ClientResponse::CommitOffsetsOk => Payload::CommitOffsetsOk,
            ClientResponse::ListCommittedOffsetsOk(ListCommittedOffsetsOkRes { offsets }) => {
                Payload::ListCommittedOffsetsOk { offsets }
            }
        }
    }
}

impl From<InterNodeReq> for Payload {
    fn from(req: InterNodeReq) -> Self {
        match req {
            InterNodeReq::GossipDigest(GossipDigestReq { digest, .. }) => {
                Payload::GossipDigest { digest }
            }
            InterNodeReq::GossipDiff(GossipDiffReq { msgs }) => Payload::GossipDiff { msgs },
        }
    }
}

impl Outgoing {
    pub fn into_msg(self, node_id: &str) -> Msg<Payload> {
        match self {
            Outgoing::KvRequest { req, msg_id } => Msg {
                src: node_id.to_string(),
                dest: "lin-kv".to_string(),
                body: Body {
                    id: Some(msg_id),
                    in_reply_to: None,
                    payload: req.into(),
                },
            },
            Outgoing::ClientResponse {
                res,
                in_reply_to,
                dest,
                msg_id,
            } => Msg {
                src: node_id.to_string(),
                dest,
                body: Body {
                    id: Some(msg_id),
                    in_reply_to: Some(in_reply_to),
                    payload: res.into(),
                },
            },
            Outgoing::InterNodeReq { req, dest, msg_id } => Msg {
                src: node_id.to_string(),
                dest,
                body: Body {
                    id: Some(msg_id),
                    payload: req.into(),
                    in_reply_to: None,
                },
            },
        }
    }
}
impl<P: Serialize> Msg<P> {
    pub fn into_reply(self) -> Msg<P> {
        Msg {
            src: self.dest,
            dest: self.src,
            body: Body {
                id: None,
                in_reply_to: self.body.id,
                payload: self.body.payload,
            },
        }
    }
}

pub enum DropReason {
    NoMsgId,
    NoInReplyTo,
    Ignored,
}

impl Msg<Payload> {
    pub fn classify(self) -> Result<Incoming, DropReason> {
        let msg_id = self.body.id;
        match self.body.payload {
            Payload::Send { key, msg } => {
                let msg_id = self.body.id.ok_or(DropReason::NoMsgId)?;
                let src = self.src;
                Ok(Incoming::ClientRequest(ClientRequest::Send(SendReq {
                    key,
                    msg,
                    msg_id,
                    src,
                })))
            }
            Payload::Poll { offsets } => {
                let msg_id = self.body.id.ok_or(DropReason::NoMsgId)?;
                let src = self.src;
                Ok(Incoming::ClientRequest(ClientRequest::Poll(PollReq {
                    offsets,
                    msg_id,
                    src,
                })))
            }
            Payload::CommitOffsets { offsets } => {
                let msg_id = msg_id.ok_or(DropReason::NoMsgId)?;
                let src = self.src;
                Ok(Incoming::ClientRequest(ClientRequest::CommitOffsets(
                    CommitReq {
                        offsets,
                        msg_id,
                        src,
                    },
                )))
            }
            Payload::ListCommittedOffsets { keys } => {
                let msg_id = msg_id.ok_or(DropReason::NoMsgId)?;
                let src = self.src;
                Ok(Incoming::ClientRequest(
                    ClientRequest::ListCommittedOffsets(ListReq { keys, msg_id, src }),
                ))
            }
            Payload::CasOk => {
                let in_reply_to = self.body.in_reply_to.ok_or(DropReason::NoInReplyTo)?;
                Ok(Incoming::KvEvent(KvEvent::CasRes(CasRes::Ok(CasOkRes {
                    in_reply_to,
                }))))
            }
            Payload::ReadOk { value } => {
                let in_reply_to = self.body.in_reply_to.ok_or(DropReason::NoInReplyTo)?;
                Ok(Incoming::KvEvent(KvEvent::ReadRes(ReadRes {
                    in_reply_to,
                    value,
                })))
            }
            Payload::Error { code, text } => {
                let in_reply_to = self.body.in_reply_to.ok_or(DropReason::NoInReplyTo)?;

                let event = match code {
                    22 => CasRes::Retry(CasRetryRes { in_reply_to }),
                    _ => CasRes::Unknown(CasResUnknown {
                        code,
                        text,
                        in_reply_to,
                    }),
                };

                Ok(Incoming::KvEvent(KvEvent::CasRes(event)))
            }
            Payload::GossipDigest { digest } => {
                let src = self.src;
                Ok(Incoming::NodeRequest(InterNodeReq::GossipDigest(
                    GossipDigestReq { digest, src },
                )))
            }
            Payload::GossipDiff { msgs } => Ok(Incoming::NodeRequest(InterNodeReq::GossipDiff(
                GossipDiffReq { msgs },
            ))),
            Payload::SendOk { .. }
            | Payload::PollOk { .. }
            | Payload::CommitOffsetsOk
            | Payload::ListCommittedOffsetsOk { .. }
            | Payload::Cas { .. }
            | Payload::Read { .. } => Err(DropReason::Ignored),
        }
    }
}
