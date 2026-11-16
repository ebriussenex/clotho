use std::collections::HashMap;

use tokio_util::bytes::Bytes;

use crate::{
    app::{
        Command, CommandError, CommandResponse, CommitOffsetsCommand, ListCommittedOffsetsCommand,
        ListCommittedOffsetsRes, PollCommand, PollRes, SendCommand, SendRes,
    },
    maelstrom::protocol::{
        value::{Base64Bytes, JsonValue},
        wire::{self, Body, ForwardedPayload, ForwardedResponse, Msg, Payload},
    },
    transport::RequestId,
};

use super::error_code;

pub enum Outgoing {
    /// Reply to a client, in the client protocol.
    CommandReply {
        msg_id: u64,
        response: CommandResponse,
        in_reply_to: u64,
        dest: String,
    },
    /// Reply to a peer's `fwd`.
    ForwardReply {
        msg_id: u64,
        response: CommandResponse,
        in_reply_to: u64,
        dest: String,
    },
    /// Error reply; the same for clients and peers.
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
            Command::Send(SendCommand { key, msg }) => ForwardedPayload::Send {
                key,
                msg: Base64Bytes(msg),
            },
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
            CommandResponse::Poll(PollRes { msgs }) => Payload::PollOk {
                msgs: numbered(msgs),
            },
            CommandResponse::CommitOffsets(_) => Payload::CommitOffsetsOk,
            CommandResponse::ListCommittedOffsets(ListCommittedOffsetsRes { offsets }) => {
                Payload::ListCommittedOffsetsOk { offsets }
            }
        }
    }
}

impl From<CommandResponse> for ForwardedResponse {
    fn from(res: CommandResponse) -> Self {
        match res {
            CommandResponse::Send(SendRes { offset }) => ForwardedResponse::SendOk { offset },
            CommandResponse::Poll(PollRes { msgs }) => ForwardedResponse::PollOk {
                msgs: super::map_msgs(msgs, Base64Bytes),
            },
            CommandResponse::CommitOffsets(_) => ForwardedResponse::CommitOffsetsOk,
            CommandResponse::ListCommittedOffsets(ListCommittedOffsetsRes { offsets }) => {
                ForwardedResponse::ListCommittedOffsetsOk { offsets }
            }
        }
    }
}

/// The client protocol lists every message with its own offset.
fn numbered(msgs: HashMap<String, (u64, Vec<Bytes>)>) -> HashMap<String, Vec<(u64, JsonValue)>> {
    msgs.into_iter()
        .map(|(key, (from, entries))| {
            let entries = entries
                .into_iter()
                .enumerate()
                .map(|(idx, msg)| (from.saturating_add(idx as u64), JsonValue(msg)))
                .collect();
            (key, entries)
        })
        .collect()
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
            Outgoing::ForwardReply {
                response,
                in_reply_to,
                dest,
                msg_id,
            } => {
                let payload = Payload::FwdOk {
                    payload: response.into(),
                };
                reply_msg(node_id, dest, msg_id, in_reply_to, payload)
            }
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
    use serde_json::{Value, json};

    use super::*;

    fn body(outgoing: Outgoing) -> Value {
        let msg = outgoing.into_msg("n1");
        let mut value = serde_json::to_value(&msg).expect("serializable");
        value["body"].take()
    }

    fn poll_res(msg: &'static [u8]) -> CommandResponse {
        CommandResponse::Poll(PollRes {
            msgs: HashMap::from([("k".to_owned(), (3, vec![Bytes::from_static(msg)]))]),
        })
    }

    #[test]
    fn forward_serializes_as_nested_fwd_with_base64_message() {
        let msg = Outgoing::Forward {
            command: Command::Send(SendCommand {
                key: "k1".to_owned(),
                msg: Bytes::from_static(b"7"),
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
                    "payload": {"type": "send", "key": "k1", "msg": "Nw=="}
                }
            })
        );
    }

    #[test]
    fn client_poll_reply_writes_messages_as_json_values() {
        let reply = Outgoing::CommandReply {
            msg_id: 1,
            response: poll_res(br#"{"a":1}"#),
            in_reply_to: 9,
            dest: "c1".to_owned(),
        };

        assert_eq!(
            body(reply),
            json!({
                "type": "poll_ok",
                "msg_id": 1,
                "in_reply_to": 9,
                "msgs": {"k": [[3, {"a": 1}]]}
            })
        );
    }

    #[test]
    fn client_messages_are_numbered_from_the_first_offset() {
        let msgs = HashMap::from([(
            "k".to_owned(),
            (7, vec![Bytes::from_static(b"1"), Bytes::from_static(b"2")]),
        )]);

        let numbered = numbered(msgs);
        assert_eq!(
            numbered["k"],
            vec![
                (7, JsonValue(Bytes::from_static(b"1"))),
                (8, JsonValue(Bytes::from_static(b"2")))
            ]
        );
    }

    #[test]
    fn client_poll_reply_with_non_json_message_fails_to_serialize() {
        let reply = Outgoing::CommandReply {
            msg_id: 1,
            response: poll_res(&[0xff]),
            in_reply_to: 9,
            dest: "c1".to_owned(),
        };

        assert!(serde_json::to_string(&reply.into_msg("n1")).is_err());
    }

    #[test]
    fn peer_reply_is_nested_fwd_ok_with_base64_messages() {
        let reply = Outgoing::ForwardReply {
            msg_id: 1,
            response: poll_res(&[0xff]),
            in_reply_to: 9,
            dest: "n2".to_owned(),
        };

        assert_eq!(
            body(reply),
            json!({
                "type": "fwd_ok",
                "msg_id": 1,
                "in_reply_to": 9,
                "payload": {"type": "poll_ok", "msgs": {"k": [3, ["/w=="]]}}
            })
        );
    }
}
