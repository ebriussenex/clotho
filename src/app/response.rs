use std::collections::HashMap;

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
    CommitOffsets,
    ListCommittedOffsets(ListCommittedOffsetsRes),
}

#[derive(Debug, Clone)]
pub struct SendRes {
    pub offset: u64,
}

#[derive(Debug, Clone)]
pub struct PollRes {
    pub msgs: HashMap<String, Vec<(u64, u32)>>,
}

#[derive(Debug, Clone)]
pub struct ListCommittedOffsetsRes {
    pub offsets: HashMap<String, u64>,
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

/// Kinds of command failures. "Definite" means the command was certainly not applied,
/// "indefinite" means it may or may not have been.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorKind {
    /// Indefinite: no answer in time, e.g. the owner of the key was unreachable.
    Timeout,
    /// Definite: the node can't serve the command right now (e.g. shutting down); retryable.
    Unavailable,
    /// Definite: the command is malformed or not supported.
    Protocol,
    /// Indefinite: an unexpected failure, e.g. a peer replied with a response of a wrong type.
    Internal,
}

impl ErrorKind {
    /// Whether the same request may be sent again in hope of a different outcome.
    pub fn is_retryable(self) -> bool {
        matches!(self, ErrorKind::Unavailable)
    }
}
