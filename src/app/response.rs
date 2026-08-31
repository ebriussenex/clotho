use std::collections::HashMap;

use crate::domain::{LogId, Offset};

#[derive(Debug)]
pub struct CommandReply {
    pub src: String,
    pub in_reply_to: usize,
    pub response: CommandResponse,
}

#[derive(Debug)]
pub enum CommandResponse {
    Send(SendRes),
    Poll(PollRes),
    CommitOffsets,
    ListCommittedOffsets(ListCommittedOffsetsRes),
}

#[derive(Debug)]
pub struct SendRes {
    pub offset: Offset,
}

#[derive(Debug)]
pub struct PollRes {
    pub msgs: HashMap<LogId, Vec<(Offset, u32)>>,
}

#[derive(Debug)]
pub struct ListCommittedOffsetsRes {
    pub offsets: HashMap<LogId, Offset>,
}
