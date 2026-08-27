use std::collections::HashMap;

pub enum ReplicationRequest {
    PeerLogDigest(PeerLogDigest),
    LogDelta(LogDelta),
}

pub struct PeerLogDigest {
    pub peer_id: String,
    pub offsets: HashMap<String, usize>,
}

pub struct LogDelta {
    pub msgs: HashMap<String, Vec<(usize, Vec<u32>)>>,
}
