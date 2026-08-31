use crate::maelstrom::protocol::wire::Envelope;
use crate::{
    app::{
        AppCommand, Command, CommandReply, CommandResponse, CommitOffsetsCommand,
        ListCommittedOffsetsCommand, ListCommittedOffsetsRes, PollCommand, PollRes, ReplyContext,
        SendCommand, SendRes,
    },
    domain::{LogId, Offset},
    maelstrom::protocol::wire::{Msg, Payload},
    replication::{LogDelta, PeerLogDigest, ReplicationCommand},
};

pub enum Incoming {
    AppCommand(AppCommand),
    CommandReply(CommandReply),
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
            Payload::SendOk { offset } => command_reply(
                envelope,
                CommandResponse::Send(SendRes {
                    offset: Offset(offset as u64),
                }),
            ),
            Payload::PollOk { msgs } => command_reply(
                envelope,
                CommandResponse::Poll(PollRes {
                    msgs: msgs
                        .into_iter()
                        .map(|(key, msgs)| {
                            (
                                LogId(key),
                                msgs.into_iter()
                                    .map(|(offset, msg)| (Offset(offset as u64), msg))
                                    .collect(),
                            )
                        })
                        .collect(),
                }),
            ),
            Payload::CommitOffsetsOk => command_reply(envelope, CommandResponse::CommitOffsets),
            Payload::ListCommittedOffsetsOk { offsets } => command_reply(
                envelope,
                CommandResponse::ListCommittedOffsets(ListCommittedOffsetsRes {
                    offsets: offsets
                        .into_iter()
                        .map(|(key, offset)| (LogId(key), Offset(offset as u64)))
                        .collect(),
                }),
            ),
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
            Payload::Error { .. } => Err(DropReason::Ignored),
        }
    }
}

fn command_reply(envelope: Envelope, response: CommandResponse) -> Result<Incoming, DropReason> {
    Ok(Incoming::CommandReply(CommandReply {
        src: envelope.src,
        in_reply_to: envelope.in_reply_to.ok_or(DropReason::NoInReplyTo)?,
        response,
    }))
}

fn reply_context(envelope: Envelope) -> Result<ReplyContext, DropReason> {
    let in_reply_to = envelope.msg_id.ok_or(DropReason::NoMsgId)?;

    Ok(ReplyContext {
        dest: envelope.src,
        in_reply_to,
    })
}
