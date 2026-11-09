use crate::{
    app::{
        AppCommand, Command, CommandError, CommandReply, CommandResponse, CommandSource,
        CommitOffsetsCommand, ListCommittedOffsetsCommand, ListCommittedOffsetsRes, PollCommand,
        PollRes, ReplyContext, SendCommand, SendRes,
    },
    maelstrom::protocol::wire::{self, Envelope, ForwardedPayload, Msg, Payload},
    transport::{PeerRequest, RequestId},
};

use super::error_code;

pub enum Incoming {
    AppCommand(AppCommand),
    CommandReply(CommandReply),
}

pub enum DropReason {
    NoMsgId,
    NoInReplyTo,
}

impl Msg<Payload> {
    pub fn classify(self) -> Result<Incoming, DropReason> {
        let envelope = self.envelope();
        match self.body.payload {
            Payload::Send { key, msg } => client_command(
                envelope,
                Command::Send(SendCommand { key, msg }),
            ),
            Payload::Poll { offsets } => {
                client_command(envelope, Command::Poll(PollCommand { offsets }))
            }
            Payload::CommitOffsets { offsets } => client_command(
                envelope,
                Command::CommitOffsets(CommitOffsetsCommand { offsets }),
            ),
            Payload::ListCommittedOffsets { keys } => client_command(
                envelope,
                Command::ListCommittedOffsets(ListCommittedOffsetsCommand { keys }),
            ),
            Payload::Fwd {
                request_id,
                watermark,
                payload,
            } => {
                let request = PeerRequest {
                    id: request_id.into(),
                    watermark,
                };
                app_command(envelope, CommandSource::Peer, Some(request), payload.into())
            }
            Payload::SendOk { offset } => {
                command_reply(envelope, Ok(CommandResponse::Send(SendRes { offset })))
            }
            Payload::PollOk { msgs } => {
                command_reply(envelope, Ok(CommandResponse::Poll(PollRes { msgs })))
            }
            Payload::CommitOffsetsOk => command_reply(envelope, Ok(CommandResponse::CommitOffsets)),
            Payload::ListCommittedOffsetsOk { offsets } => command_reply(
                envelope,
                Ok(CommandResponse::ListCommittedOffsets(
                    ListCommittedOffsetsRes { offsets },
                )),
            ),
            Payload::Error { code, text } => command_reply(
                envelope,
                Err(CommandError::new(error_code::from_code(code), text)),
            ),
        }
    }
}

impl From<ForwardedPayload> for Command {
    fn from(payload: ForwardedPayload) -> Self {
        match payload {
            ForwardedPayload::Send { key, msg } => Command::Send(SendCommand { key, msg }),
            ForwardedPayload::Poll { offsets } => Command::Poll(PollCommand { offsets }),
            ForwardedPayload::CommitOffsets { offsets } => {
                Command::CommitOffsets(CommitOffsetsCommand { offsets })
            }
            ForwardedPayload::ListCommittedOffsets { keys } => {
                Command::ListCommittedOffsets(ListCommittedOffsetsCommand { keys })
            }
        }
    }
}

impl From<wire::RequestId> for RequestId {
    fn from(id: wire::RequestId) -> Self {
        RequestId {
            incarnation: id.incarnation,
            seq: id.seq,
        }
    }
}

fn client_command(envelope: Envelope, kind: Command) -> Result<Incoming, DropReason> {
    app_command(envelope, CommandSource::Client, None, kind)
}

fn app_command(
    envelope: Envelope,
    source: CommandSource,
    request: Option<PeerRequest>,
    kind: Command,
) -> Result<Incoming, DropReason> {
    let in_reply_to = envelope.msg_id.ok_or(DropReason::NoMsgId)?;
    Ok(Incoming::AppCommand(AppCommand {
        reply_ctx: ReplyContext {
            dest: envelope.src,
            in_reply_to,
            request,
        },
        kind,
        source,
    }))
}

fn command_reply(
    envelope: Envelope,
    result: Result<CommandResponse, CommandError>,
) -> Result<Incoming, DropReason> {
    Ok(Incoming::CommandReply(CommandReply {
        src: envelope.src,
        in_reply_to: envelope.in_reply_to.ok_or(DropReason::NoInReplyTo)?,
        result,
    }))
}
