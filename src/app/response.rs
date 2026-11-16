use std::collections::HashMap;

use tokio_util::bytes::Bytes;

#[derive(Debug)]
pub struct CommandReply {
    pub src: String,
    pub in_reply_to: u64,
    pub result: Result<CommandResponse, CommandError>,
}

#[derive(Debug, Clone)]
pub enum CommandResponse {
    Send(SendRes),
    Poll(PollRes),
    CommitOffsets(CommitOffsetsRes),
    ListCommittedOffsets(ListCommittedOffsetsRes),
}

#[derive(Debug, Clone)]
pub struct SendRes {
    pub offset: u64,
}

#[derive(Debug, Clone)]
pub struct PollRes {
    /// Messages of each key, the first at the offset given with them.
    pub msgs: HashMap<String, (u64, Vec<Bytes>)>,
}

#[derive(Debug, Clone)]
pub struct CommitOffsetsRes;

#[derive(Debug, Clone)]
pub struct ListCommittedOffsetsRes {
    pub offsets: HashMap<String, u64>,
}

impl TryFrom<CommandResponse> for SendRes {
    type Error = CommandResponse;

    fn try_from(response: CommandResponse) -> Result<Self, Self::Error> {
        match response {
            CommandResponse::Send(res) => Ok(res),
            response => Err(response),
        }
    }
}

impl TryFrom<CommandResponse> for PollRes {
    type Error = CommandResponse;

    fn try_from(response: CommandResponse) -> Result<Self, Self::Error> {
        match response {
            CommandResponse::Poll(res) => Ok(res),
            response => Err(response),
        }
    }
}

impl TryFrom<CommandResponse> for CommitOffsetsRes {
    type Error = CommandResponse;

    fn try_from(response: CommandResponse) -> Result<Self, Self::Error> {
        match response {
            CommandResponse::CommitOffsets(res) => Ok(res),
            response => Err(response),
        }
    }
}

impl TryFrom<CommandResponse> for ListCommittedOffsetsRes {
    type Error = CommandResponse;

    fn try_from(response: CommandResponse) -> Result<Self, Self::Error> {
        match response {
            CommandResponse::ListCommittedOffsets(res) => Ok(res),
            response => Err(response),
        }
    }
}

/// Error reply to a command, sent to a client or received from a peer.
#[derive(Debug, Clone)]
pub struct CommandError {
    pub kind: ErrorKind,
    pub text: String,
}

impl CommandError {
    pub fn new(kind: ErrorKind, text: impl Into<String>) -> Self {
        Self {
            kind,
            text: text.into(),
        }
    }
}

/// Why a command failed. *Definite* kinds mean the command was certainly not applied,
/// *indefinite* ones that it may or may not have been.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorKind {
    /// Indefinite: no answer in time, e.g. the owner of a key was unreachable.
    Timeout,
    /// Definite and retryable: the node can't take the command now (overloaded, shutting down).
    Unavailable,
    /// Definite: the command is malformed or not supported.
    Protocol,
    /// Indefinite: a bug, e.g. a peer replied with a response of a wrong type.
    Internal,
}

impl ErrorKind {
    /// Whether sending the same request again may succeed.
    pub fn is_retryable(self) -> bool {
        matches!(self, ErrorKind::Unavailable)
    }

    /// Whether the command was certainly not applied.
    pub fn is_definite(self) -> bool {
        matches!(self, ErrorKind::Unavailable | ErrorKind::Protocol)
    }
}
