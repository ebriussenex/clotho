use std::collections::HashMap;

use crate::domain::{LogId, Offset};

#[derive(Debug, Clone)]
pub struct ReplyContext {
    pub dest: String,
    pub in_reply_to: usize,
}

pub struct AppCommand {
    pub reply_ctx: ReplyContext,
    pub kind: Command,
}

pub enum Command {
    Send(SendCommand),
    Poll(PollCommand),
    CommitOffsets(CommitOffsetsCommand),
    ListCommittedOffsets(ListCommittedOffsetsCommand),
}

pub struct SendCommand {
    pub key: LogId,
    pub msg: u32,
}

pub struct PollCommand {
    pub offsets: HashMap<LogId, Offset>,
}

#[derive(Debug, Clone)]
pub struct CommitOffsetsCommand {
    pub offsets: HashMap<LogId, Offset>,
}

#[derive(Debug, Clone)]
pub struct ListCommittedOffsetsCommand {
    pub keys: Vec<LogId>,
}
