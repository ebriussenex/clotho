use std::collections::HashMap;

use crate::domain::{LogId, Offset};

pub enum ClientResponse {
    Send(SendRes),
    Poll(PollRes),
    CommitOffsets,
    ListCommittedOffsets(ListCommittedOffsetsRes),
}

pub struct SendRes {
    pub offset: Offset,
}

pub struct PollRes {
    pub msgs: HashMap<LogId, Vec<(Offset, u32)>>,
}

pub struct ListCommittedOffsetsRes {
    pub offsets: HashMap<LogId, Offset>,
}
