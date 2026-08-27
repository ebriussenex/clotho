use std::collections::HashMap;

use crate::domain::{LogId, Offset};

#[derive(Debug, Clone)]
pub struct CommitOffsetsCommand {
    pub offsets: HashMap<LogId, Offset>,
}

#[derive(Debug, Clone)]
pub struct ListCommittedOffsetsCommand {
    pub keys: Vec<LogId>,
}
