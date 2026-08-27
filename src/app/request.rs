use std::collections::HashMap;

use crate::{
    offsets::command::{CommitOffsetsCommand, ListCommittedOffsetsCommand},
    protocol::envelope::RequestContext,
};

pub struct Request<Command> {
    pub ctx: RequestContext,
    pub command: Command,
}

pub enum ClientRequest {
    Send(SendReq),
    Poll(PollReq),
    CommitOffsets(CommitReq),
    ListCommittedOffsets(ListReq),
}

pub type SendReq = Request<SendCommand>;
pub type PollReq = Request<PollCommand>;
pub type CommitReq = Request<CommitOffsetsCommand>;
pub type ListReq = Request<ListCommittedOffsetsCommand>;

pub struct SendCommand {
    pub key: String,
    pub msg: u32,
}

pub struct PollCommand {
    pub offsets: HashMap<String, usize>,
}
