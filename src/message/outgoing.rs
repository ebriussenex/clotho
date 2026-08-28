use crate::{
    app::{ClientResponse, ListCommittedOffsetsRes, PollRes, SendRes},
    kv::{CasReq, KvRequest, ReadReq},
    maelstrom::protocol::wire::{Body, Msg, Payload},
    replication::{LogDelta, ReplicationMessage},
};

pub enum Outgoing {
    KvRequest {
        request: KvRequest,
        msg_id: usize,
    },
    ClientResponse {
        msg_id: usize,
        res: ClientResponse,
        in_reply_to: usize,
        dest: String,
    },
    Replication {
        message: ReplicationMessage,
        msg_id: usize,
        dest: String,
    },
}

impl From<KvRequest> for Payload {
    fn from(request: KvRequest) -> Self {
        match request {
            KvRequest::Cas(CasReq { key, from, to, .. }) => Payload::Cas {
                key: key.0,
                from: from.0 as usize,
                to: to.0 as usize,
                create_if_not_exists: true,
            },

            KvRequest::Read(ReadReq { key, .. }) => Payload::Read { key: key.0 },
        }
    }
}

impl From<ClientResponse> for Payload {
    fn from(res: ClientResponse) -> Self {
        match res {
            ClientResponse::Send(SendRes { offset }) => Payload::SendOk {
                offset: offset.0 as usize,
            },
            ClientResponse::Poll(PollRes { msgs }) => Payload::PollOk {
                msgs: msgs
                    .into_iter()
                    .map(|(key, msgs)| {
                        (
                            key.0,
                            msgs.into_iter()
                                .map(|(offset, msg)| (offset.0 as usize, msg))
                                .collect(),
                        )
                    })
                    .collect(),
            },
            ClientResponse::CommitOffsets => Payload::CommitOffsetsOk,
            ClientResponse::ListCommittedOffsets(ListCommittedOffsetsRes { offsets }) => {
                Payload::ListCommittedOffsetsOk {
                    offsets: offsets
                        .into_iter()
                        .map(|(key, offset)| (key.0, offset.0 as usize))
                        .collect(),
                }
            }
        }
    }
}

impl From<ReplicationMessage> for Payload {
    fn from(message: ReplicationMessage) -> Self {
        match message {
            ReplicationMessage::LogDigest(offsets) => Payload::GossipDigest {
                digest: offsets
                    .into_iter()
                    .map(|(key, offset)| (key.0, offset.0 as usize))
                    .collect(),
            },
            ReplicationMessage::LogDelta(LogDelta { msgs }) => Payload::GossipDiff {
                msgs: msgs
                    .into_iter()
                    .map(|(key, chunks)| {
                        (
                            key.0,
                            chunks
                                .into_iter()
                                .map(|(offset, msgs)| (offset.0 as usize, msgs))
                                .collect(),
                        )
                    })
                    .collect(),
            },
        }
    }
}

impl Outgoing {
    pub fn into_msg(self, node_id: &str) -> Msg<Payload> {
        match self {
            Outgoing::KvRequest { request, msg_id } => Msg {
                src: node_id.to_string(),
                dest: "lin-kv".to_string(),
                body: Body {
                    id: Some(msg_id),
                    in_reply_to: None,
                    payload: request.into(),
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
            Outgoing::Replication {
                message,
                dest,
                msg_id,
            } => Msg {
                src: node_id.to_string(),
                dest,
                body: Body {
                    id: Some(msg_id),
                    payload: message.into(),
                    in_reply_to: None,
                },
            },
        }
    }
}
