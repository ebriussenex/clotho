use std::collections::HashMap;

use crate::domain::{LogId, Offset};

pub enum ReplicationCommand {
    PeerLogDigest(PeerLogDigest),
    LogDelta(LogDelta),
}

pub struct PeerLogDigest {
    pub peer_id: String,
    pub offsets: HashMap<LogId, Offset>,
}

pub struct LogDelta {
    pub msgs: HashMap<LogId, Vec<(Offset, Vec<u32>)>>,
}
