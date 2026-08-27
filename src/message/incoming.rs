use crate::{
    app::{ClientRequest, PollCommand, Request, SendCommand},
    domain::{LogId, Offset},
    kv::{CasOkRes, CasRes, CasResUnknown, CasRetryRes, KvEvent, ReadRes},
    offsets::command::{CommitOffsetsCommand, ListCommittedOffsetsCommand},
    protocol::{
        envelope::{Envelope, RequestContext},
        maelstrom::{Msg, Payload},
    },
    replication::{LogDelta, PeerLogDigest, ReplicationRequest},
};

pub enum Incoming {
    ClientRequest(ClientRequest),
    KvEvent(KvEvent),
    ReplicationRequest(ReplicationRequest),
}

pub enum DropReason {
    NoMsgId,
    NoInReplyTo,
    Ignored,
}

impl Msg<Payload> {
    pub fn classify(self) -> Result<Incoming, DropReason> {
        let envelope = self.envelope();
        match self.body.payload {
            Payload::Send { key, msg } => {
                let ctx = request_context(envelope)?;
                Ok(Incoming::ClientRequest(ClientRequest::Send(Request {
                    ctx,
                    command: SendCommand { key, msg },
                })))
            }
            Payload::Poll { offsets } => {
                let ctx = request_context(envelope)?;
                Ok(Incoming::ClientRequest(ClientRequest::Poll(Request {
                    ctx,
                    command: PollCommand { offsets },
                })))
            }
            Payload::CommitOffsets { offsets } => {
                let ctx = request_context(envelope)?;
                Ok(Incoming::ClientRequest(ClientRequest::CommitOffsets(
                    Request {
                        ctx,
                        command: CommitOffsetsCommand {
                            offsets: offsets
                                .into_iter()
                                .map(|(key, offset)| (LogId(key), Offset(offset as u64)))
                                .collect(),
                        },
                    },
                )))
            }
            Payload::ListCommittedOffsets { keys } => {
                let ctx = request_context(envelope)?;
                Ok(Incoming::ClientRequest(
                    ClientRequest::ListCommittedOffsets(Request {
                        ctx,
                        command: ListCommittedOffsetsCommand {
                            keys: keys.into_iter().map(LogId).collect(),
                        },
                    }),
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
                Ok(Incoming::ReplicationRequest(
                    ReplicationRequest::PeerLogDigest(PeerLogDigest {
                        peer_id: src,
                        offsets: digest,
                    }),
                ))
            }
            Payload::GossipDiff { msgs } => Ok(Incoming::ReplicationRequest(
                ReplicationRequest::LogDelta(LogDelta { msgs }),
            )),
            Payload::SendOk { .. }
            | Payload::PollOk { .. }
            | Payload::CommitOffsetsOk
            | Payload::ListCommittedOffsetsOk { .. }
            | Payload::Cas { .. }
            | Payload::Read { .. } => Err(DropReason::Ignored),
        }
    }
}

fn request_context(envelope: Envelope) -> Result<RequestContext, DropReason> {
    let in_reply_to = envelope.msg_id.ok_or(DropReason::NoMsgId)?;

    Ok(RequestContext {
        reply_to: envelope.src,
        in_reply_to,
    })
}
