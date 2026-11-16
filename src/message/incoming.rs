use crate::{
    app::{
        AppCommand, Command, CommandError, CommandReply, CommandResponse, CommandSource,
        CommitOffsetsCommand, CommitOffsetsRes, ListCommittedOffsetsCommand,
        ListCommittedOffsetsRes, PollCommand, PollRes, ReplyContext, SendCommand, SendRes,
    },
    maelstrom::protocol::wire::{
        self, Envelope, ForwardedPayload, ForwardedResponse, Msg, Payload,
    },
    transport::{PeerRequest, RequestId},
};

use super::error_code;

pub enum Incoming {
    AppCommand(AppCommand),
    CommandReply(CommandReply),
}

/// Why a message is dropped without a reply.
pub enum DropReason {
    /// A command without `msg_id` can't be replied to.
    NoMsgId,
    /// A reply without `in_reply_to` can't be matched to a request.
    NoInReplyTo,
    /// A reply of the client protocol: the node never sends client requests, so none is awaited.
    ClientReply,
}

impl Msg<Payload> {
    /// Splits a message into a command to execute or a reply to a request of this node.
    ///
    /// # Errors
    ///
    /// The [`DropReason`] if the message can't be handled and must be dropped.
    pub fn classify(self) -> Result<Incoming, DropReason> {
        let envelope = self.envelope();
        match self.body.payload {
            Payload::Send { key, msg } => {
                client_command(envelope, Command::Send(SendCommand { key, msg: msg.0 }))
            }
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
                let command = Command::from(payload);
                let request = PeerRequest {
                    id: request_id.into(),
                    watermark,
                    deduplicate: !command.is_read_only(),
                };
                app_command(envelope, CommandSource::Peer, Some(request), command)
            }
            Payload::FwdOk { payload } => command_reply(envelope, Ok(payload.into())),
            Payload::SendOk { .. }
            | Payload::PollOk { .. }
            | Payload::CommitOffsetsOk
            | Payload::ListCommittedOffsetsOk { .. } => Err(DropReason::ClientReply),
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
            ForwardedPayload::Send { key, msg } => Command::Send(SendCommand { key, msg: msg.0 }),
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

impl From<ForwardedResponse> for CommandResponse {
    fn from(response: ForwardedResponse) -> Self {
        match response {
            ForwardedResponse::SendOk { offset } => CommandResponse::Send(SendRes { offset }),
            ForwardedResponse::PollOk { msgs } => CommandResponse::Poll(PollRes {
                msgs: super::map_msgs(msgs, |msg| msg.0),
            }),
            ForwardedResponse::CommitOffsetsOk => CommandResponse::CommitOffsets(CommitOffsetsRes),
            ForwardedResponse::ListCommittedOffsetsOk { offsets } => {
                CommandResponse::ListCommittedOffsets(ListCommittedOffsetsRes { offsets })
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

#[cfg(test)]
mod tests {
    use tokio_util::bytes::Bytes;

    use super::*;

    fn classify(json: &str) -> Result<Incoming, DropReason> {
        serde_json::from_str::<Msg<Payload>>(json)
            .expect("valid message")
            .classify()
    }

    #[test]
    fn client_message_becomes_its_json_text() {
        let incoming = classify(
            r#"{"src":"c1","dest":"n1","body":{"type":"send","msg_id":1,"key":"k","msg":{"a":[1]}}}"#,
        );

        let Ok(Incoming::AppCommand(AppCommand {
            kind: Command::Send(SendCommand { msg, .. }),
            ..
        })) = incoming
        else {
            panic!("expected a send command");
        };
        assert_eq!(msg, Bytes::from_static(br#"{"a":[1]}"#));
    }

    #[test]
    fn fwd_ok_becomes_reply_with_raw_bytes() {
        let incoming = classify(
            r#"{"src":"n2","dest":"n1","body":{"type":"fwd_ok","msg_id":5,"in_reply_to":9,
                "payload":{"type":"poll_ok","msgs":{"k":[3,["/w=="]]}}}}"#,
        );

        let Ok(Incoming::CommandReply(CommandReply {
            in_reply_to: 9,
            result: Ok(CommandResponse::Poll(PollRes { msgs })),
            ..
        })) = incoming
        else {
            panic!("expected a poll reply");
        };
        assert_eq!(msgs["k"], (3, vec![Bytes::from_static(&[0xff])]));
    }

    #[test]
    fn client_protocol_reply_is_dropped() {
        let incoming = classify(
            r#"{"src":"n2","dest":"n1","body":{"type":"send_ok","msg_id":5,"in_reply_to":9,"offset":1}}"#,
        );

        assert!(matches!(incoming, Err(DropReason::ClientReply)));
    }

    fn forwarded_request(payload: &str) -> Option<PeerRequest> {
        let incoming = classify(&format!(
            r#"{{"src":"n2","dest":"n1","body":{{"type":"fwd","msg_id":5,
                "request_id":{{"incarnation":1,"seq":3}},"watermark":3,"payload":{payload}}}}}"#
        ));
        match incoming {
            Ok(Incoming::AppCommand(command)) => command.reply_ctx.request,
            _ => None,
        }
    }

    #[test]
    fn only_writes_from_peers_are_deduplicated() {
        let send = forwarded_request(r#"{"type":"send","key":"k","msg":"Nw=="}"#);
        let poll = forwarded_request(r#"{"type":"poll","offsets":{"k":0}}"#);

        assert!(matches!(
            send,
            Some(PeerRequest {
                deduplicate: true,
                ..
            })
        ));
        assert!(matches!(
            poll,
            Some(PeerRequest {
                deduplicate: false,
                ..
            })
        ));
    }
}
