use crate::{
    app::{ClientResponse, ListCommittedOffsetsRes, PollRes, SendRes},
    kv::{CasReq, KvRequest, ReadReq},
    protocol::maelstrom::{Body, Msg, Payload},
    replication::{LogDelta, PeerLogDigest, ReplicationRequest},
};

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
    ReplicationRequest {
        req: ReplicationRequest,
        msg_id: usize,
        dest: String,
    },
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
            ClientResponse::Send(SendRes { offset }) => Payload::SendOk { offset },
            ClientResponse::Poll(PollRes { msgs }) => Payload::PollOk { msgs },
            ClientResponse::CommitOffsets => Payload::CommitOffsetsOk,
            ClientResponse::ListCommittedOffsets(ListCommittedOffsetsRes { offsets }) => {
                Payload::ListCommittedOffsetsOk { offsets }
            }
        }
    }
}

impl From<ReplicationRequest> for Payload {
    fn from(req: ReplicationRequest) -> Self {
        match req {
            ReplicationRequest::PeerLogDigest(PeerLogDigest { offsets, .. }) => {
                Payload::GossipDigest { digest: offsets }
            }
            ReplicationRequest::LogDelta(LogDelta { msgs }) => Payload::GossipDiff { msgs },
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
            Outgoing::ReplicationRequest { req, dest, msg_id } => Msg {
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
