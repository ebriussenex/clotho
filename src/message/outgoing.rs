use crate::{
    app::{
        Command, CommandError, CommandResponse, CommitOffsetsCommand, ListCommittedOffsetsCommand,
        ListCommittedOffsetsRes, PollCommand, PollRes, SendCommand, SendRes,
    },
    maelstrom::protocol::wire::{self, Body, ForwardedPayload, Msg, Payload},
    transport::RequestId,
};

use super::error_code;

pub enum Outgoing {
    CommandReply {
        msg_id: u64,
        response: CommandResponse,
        in_reply_to: u64,
        dest: String,
    },
    ErrorReply {
        msg_id: u64,
        error: CommandError,
        in_reply_to: u64,
        dest: String,
    },
    Forward {
        command: Command,
        msg_id: u64,
        dest: String,
        request_id: RequestId,
        watermark: u64,
    },
}

impl From<Command> for ForwardedPayload {
    fn from(command: Command) -> Self {
        match command {
            Command::Send(SendCommand { key, msg }) => ForwardedPayload::Send { key, msg },
            Command::Poll(PollCommand { offsets }) => ForwardedPayload::Poll { offsets },
            Command::CommitOffsets(CommitOffsetsCommand { offsets }) => {
                ForwardedPayload::CommitOffsets { offsets }
            }
            Command::ListCommittedOffsets(ListCommittedOffsetsCommand { keys }) => {
                ForwardedPayload::ListCommittedOffsets { keys }
            }
        }
    }
}

impl From<CommandResponse> for Payload {
    fn from(res: CommandResponse) -> Self {
        match res {
            CommandResponse::Send(SendRes { offset }) => Payload::SendOk { offset },
            CommandResponse::Poll(PollRes { msgs }) => Payload::PollOk { msgs },
            CommandResponse::CommitOffsets => Payload::CommitOffsetsOk,
            CommandResponse::ListCommittedOffsets(ListCommittedOffsetsRes { offsets }) => {
                Payload::ListCommittedOffsetsOk { offsets }
            }
        }
    }
}

impl From<CommandError> for Payload {
    fn from(error: CommandError) -> Self {
        Payload::Error {
            code: error_code::to_code(error.kind),
            text: error.text,
        }
    }
}

impl From<RequestId> for wire::RequestId {
    fn from(id: RequestId) -> Self {
        wire::RequestId {
            incarnation: id.incarnation,
            seq: id.seq,
        }
    }
}

impl Outgoing {
    pub fn into_msg(self, node_id: &str) -> Msg<Payload> {
        match self {
            Outgoing::CommandReply {
                response,
                in_reply_to,
                dest,
                msg_id,
            } => reply_msg(node_id, dest, msg_id, in_reply_to, response.into()),
            Outgoing::ErrorReply {
                error,
                in_reply_to,
                dest,
                msg_id,
            } => reply_msg(node_id, dest, msg_id, in_reply_to, error.into()),
            Outgoing::Forward {
                command,
                msg_id,
                dest,
                request_id,
                watermark,
            } => Msg {
                src: node_id.to_string(),
                dest,
                body: Body {
                    id: Some(msg_id),
                    in_reply_to: None,
                    payload: Payload::Fwd {
                        request_id: request_id.into(),
                        watermark,
                        payload: command.into(),
                    },
                },
            },
        }
    }
}

fn reply_msg(
    node_id: &str,
    dest: String,
    msg_id: u64,
    in_reply_to: u64,
    payload: Payload,
) -> Msg<Payload> {
    Msg {
        src: node_id.to_string(),
        dest,
        body: Body {
            id: Some(msg_id),
            in_reply_to: Some(in_reply_to),
            payload,
        },
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn forward_serializes_as_nested_fwd() {
        let msg = Outgoing::Forward {
            command: Command::Send(SendCommand {
                key: "k1".to_owned(),
                msg: 7,
            }),
            msg_id: 42,
            dest: "n2".to_owned(),
            request_id: RequestId {
                incarnation: 913,
                seq: 17,
            },
            watermark: 15,
        }
        .into_msg("n1");

        let value = serde_json::to_value(&msg).expect("serializable");
        assert_eq!(
            value,
            json!({
                "src": "n1",
                "dest": "n2",
                "body": {
                    "type": "fwd",
                    "msg_id": 42,
                    "in_reply_to": null,
                    "request_id": {"incarnation": 913, "seq": 17},
                    "watermark": 15,
                    "payload": {"type": "send", "key": "k1", "msg": 7}
                }
            })
        );
    }
}
