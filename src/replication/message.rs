use std::collections::HashMap;

use crate::domain::{LogId, Offset};

use super::LogDelta;

pub enum ReplicationMessage {
    LogDigest(HashMap<LogId, Offset>),
    LogDelta(LogDelta),
}
