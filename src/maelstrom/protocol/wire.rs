use std::collections::HashMap;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone)]
pub struct Envelope {
    pub src: String,
    pub msg_id: Option<u64>,
    pub in_reply_to: Option<u64>,
}

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
            in_reply_to: self.body.in_reply_to,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Body<Payload> {
    #[serde(rename = "msg_id")]
    pub id: Option<u64>,
    pub in_reply_to: Option<u64>,
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
        offset: u64,
    },
    Poll {
        offsets: HashMap<String, u64>,
    },
    PollOk {
        msgs: HashMap<String, Vec<(u64, u32)>>,
    },
    CommitOffsets {
        offsets: HashMap<String, u64>,
    },
    CommitOffsetsOk,
    ListCommittedOffsets {
        keys: Vec<String>,
    },
    ListCommittedOffsetsOk {
        offsets: HashMap<String, u64>,
    },
    Fwd {
        request_id: RequestId,
        watermark: u64,
        payload: ForwardedPayload,
    },
    Error {
        code: u8,
        text: String,
    },
}

/// Identity of one logical internode request, stable across its retries.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct RequestId {
    pub incarnation: u64,
    pub seq: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
pub enum ForwardedPayload {
    Send { key: String, msg: u32 },
    Poll { offsets: HashMap<String, u64> },
    CommitOffsets { offsets: HashMap<String, u64> },
    ListCommittedOffsets { keys: Vec<String> },
}
