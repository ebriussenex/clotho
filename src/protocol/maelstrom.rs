use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::protocol::envelope::Envelope;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Msg<Payload> {
    pub src: String,
    pub dest: String,
    pub body: Body<Payload>,
}

impl<Payload> Msg<Payload> {
    pub fn envelope(&self) -> Envelope {
        Envelope {
            src: self.src.clone(),
            msg_id: self.body.id,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Body<Payload> {
    #[serde(rename = "msg_id")]
    pub id: Option<usize>,
    pub in_reply_to: Option<usize>,
    #[serde(flatten)]
    pub payload: Payload,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
pub enum Payload {
    Send {
        key: String,
        msg: u32,
    },
    SendOk {
        offset: usize,
    },
    Poll {
        offsets: HashMap<String, usize>,
    },
    PollOk {
        msgs: HashMap<String, Vec<(usize, u32)>>,
    },
    CommitOffsets {
        offsets: HashMap<String, usize>,
    },
    CommitOffsetsOk,
    ListCommittedOffsets {
        keys: Vec<String>,
    },
    ListCommittedOffsetsOk {
        offsets: HashMap<String, usize>,
    },
    Cas {
        key: String,
        from: usize,
        to: usize,
        create_if_not_exists: bool,
    },
    Read {
        key: String,
    },
    ReadOk {
        value: usize,
    },
    CasOk,
    Error {
        code: u8,
        text: String,
    },
    GossipDigest {
        digest: HashMap<String, usize>,
    },
    GossipDiff {
        msgs: HashMap<String, Vec<(usize, Vec<u32>)>>,
    },
}
