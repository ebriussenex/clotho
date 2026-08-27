use std::collections::HashMap;

pub enum ClientResponse {
    Send(SendRes),
    Poll(PollRes),
    CommitOffsets,
    ListCommittedOffsets(ListCommittedOffsetsRes),
}

pub struct SendRes {
    pub offset: usize,
}

pub struct PollRes {
    pub msgs: HashMap<String, Vec<(usize, u32)>>,
}

pub struct ListCommittedOffsetsRes {
    pub offsets: HashMap<String, usize>,
}
