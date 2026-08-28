use crate::maelstrom::protocol::wire::Envelope;
use crate::{
    app::{
        AppCommand, Command, CommitOffsetsCommand, ListCommittedOffsetsCommand, PollCommand,
        ReplyContext, SendCommand,
    },
    domain::{LogId, Offset},
    kv::{CasOkRes, CasRes, CasResUnknown, CasRetryRes, KvEvent, ReadRes},
    maelstrom::protocol::wire::{Msg, Payload},
    replication::{LogDelta, PeerLogDigest, ReplicationCommand},
};

pub enum Incoming {
    AppCommand(AppCommand),
    KvEvent(KvEvent),
    ReplicationCommand(ReplicationCommand),
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
                let ctx = reply_context(envelope)?;
                Ok(Incoming::AppCommand(AppCommand {
                    reply_ctx: ctx,
                    kind: Command::Send(SendCommand {
                        key: LogId(key),
                        msg,
                    }),
                }))
            }
            Payload::Poll { offsets } => {
                let ctx = reply_context(envelope)?;
                Ok(Incoming::AppCommand(AppCommand {
                    reply_ctx: ctx,
                    kind: Command::Poll(PollCommand {
                        offsets: offsets
                            .into_iter()
                            .map(|(key, offset)| (LogId(key), Offset(offset as u64)))
                            .collect(),
                    }),
                }))
            }
            Payload::CommitOffsets { offsets } => {
                let ctx = reply_context(envelope)?;
                Ok(Incoming::AppCommand(AppCommand {
                    reply_ctx: ctx,
                    kind: Command::CommitOffsets(CommitOffsetsCommand {
                        offsets: offsets
                            .into_iter()
                            .map(|(key, offset)| (LogId(key), Offset(offset as u64)))
                            .collect(),
                    }),
                }))
            }
            Payload::ListCommittedOffsets { keys } => {
                let ctx = reply_context(envelope)?;
                Ok(Incoming::AppCommand(AppCommand {
                    reply_ctx: ctx,
                    kind: Command::ListCommittedOffsets(ListCommittedOffsetsCommand {
                        keys: keys.into_iter().map(LogId).collect(),
                    }),
                }))
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
                    value: Offset(value as u64),
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
                Ok(Incoming::ReplicationCommand(
                    ReplicationCommand::PeerLogDigest(PeerLogDigest {
                        peer_id: src,
                        offsets: digest
                            .into_iter()
                            .map(|(key, offset)| (LogId(key), Offset(offset as u64)))
                            .collect(),
                    }),
                ))
            }
            Payload::GossipDiff { msgs } => Ok(Incoming::ReplicationCommand(
                ReplicationCommand::LogDelta(LogDelta {
                    msgs: msgs
                        .into_iter()
                        .map(|(key, chunks)| {
                            (
                                LogId(key),
                                chunks
                                    .into_iter()
                                    .map(|(offset, msgs)| (Offset(offset as u64), msgs))
                                    .collect(),
                            )
                        })
                        .collect(),
                }),
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

fn reply_context(envelope: Envelope) -> Result<ReplyContext, DropReason> {
    let in_reply_to = envelope.msg_id.ok_or(DropReason::NoMsgId)?;

    Ok(ReplyContext {
        dest: envelope.src,
        in_reply_to,
    })
}
