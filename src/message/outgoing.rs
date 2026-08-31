use crate::{
    app::{Command, CommandResponse, ListCommittedOffsetsRes, PollRes, SendRes},
    maelstrom::protocol::wire::{Body, Msg, Payload},
    replication::{LogDelta, ReplicationMessage},
};

#[allow(
    dead_code,
    reason = "forwarding and replication producers are not connected yet"
)]
pub enum Outgoing {
    CommandReply {
        msg_id: usize,
        response: CommandResponse,
        in_reply_to: usize,
        dest: String,
    },
    Forward {
        command: Command,
        msg_id: usize,
        dest: String,
    },
    Replication {
        message: ReplicationMessage,
        msg_id: usize,
        dest: String,
    },
}

impl From<Command> for Payload {
    fn from(command: Command) -> Self {
        match command {
            Command::Send(command) => Payload::Send {
                key: command.key.0,
                msg: command.msg,
            },
            Command::Poll(command) => Payload::Poll {
                offsets: command
                    .offsets
                    .into_iter()
                    .map(|(key, offset)| (key.0, offset.0 as usize))
                    .collect(),
            },
            Command::CommitOffsets(command) => Payload::CommitOffsets {
                offsets: command
                    .offsets
                    .into_iter()
                    .map(|(key, offset)| (key.0, offset.0 as usize))
                    .collect(),
            },
            Command::ListCommittedOffsets(command) => Payload::ListCommittedOffsets {
                keys: command.keys.into_iter().map(|key| key.0).collect(),
            },
        }
    }
}

impl From<CommandResponse> for Payload {
    fn from(res: CommandResponse) -> Self {
        match res {
            CommandResponse::Send(SendRes { offset }) => Payload::SendOk {
                offset: offset.0 as usize,
            },
            CommandResponse::Poll(PollRes { msgs }) => Payload::PollOk {
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
            CommandResponse::CommitOffsets => Payload::CommitOffsetsOk,
            CommandResponse::ListCommittedOffsets(ListCommittedOffsetsRes { offsets }) => {
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
            Outgoing::CommandReply {
                response,
                in_reply_to,
                dest,
                msg_id,
            } => Msg {
                src: node_id.to_string(),
                dest,
                body: Body {
                    id: Some(msg_id),
                    in_reply_to: Some(in_reply_to),
                    payload: response.into(),
                },
            },
            Outgoing::Forward {
                command,
                msg_id,
                dest,
            } => Msg {
                src: node_id.to_string(),
                dest,
                body: Body {
                    id: Some(msg_id),
                    in_reply_to: None,
                    payload: command.into(),
                },
            },
            Outgoing::Replication {
                message,
                msg_id,
                dest,
            } => Msg {
                src: node_id.to_string(),
                dest,
                body: Body {
                    id: Some(msg_id),
                    in_reply_to: None,
                    payload: message.into(),
                },
            },
        }
    }
}
