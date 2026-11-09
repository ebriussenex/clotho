use std::collections::HashMap;

use crate::transport::PeerRequest;

#[derive(Debug, Clone)]
pub struct ReplyContext {
    pub dest: String,
    pub in_reply_to: u64,
    pub request: Option<PeerRequest>,
}

pub struct AppCommand {
    pub reply_ctx: ReplyContext,
    pub kind: Command,
    pub source: CommandSource,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommandSource {
    Client,
    Peer,
}

#[derive(Debug, Clone)]
pub enum Command {
    Send(SendCommand),
    Poll(PollCommand),
    CommitOffsets(CommitOffsetsCommand),
    ListCommittedOffsets(ListCommittedOffsetsCommand),
}

#[derive(Debug, Clone)]
pub struct SendCommand {
    pub key: String,
    pub msg: u32,
}

#[derive(Debug, Clone)]
pub struct PollCommand {
    pub offsets: HashMap<String, u64>,
}

#[derive(Debug, Clone)]
pub struct CommitOffsetsCommand {
    pub offsets: HashMap<String, u64>,
}

#[derive(Debug, Clone)]
pub struct ListCommittedOffsetsCommand {
    pub keys: Vec<String>,
}
