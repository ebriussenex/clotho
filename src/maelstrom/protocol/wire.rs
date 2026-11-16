use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use super::value::{Base64Bytes, JsonValue};

#[derive(Debug, Clone)]
pub struct Envelope {
    pub src: String,
    pub msg_id: Option<u64>,
    pub in_reply_to: Option<u64>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct MsgHeader {
    pub src: String,
    pub body: BodyHeader,
}

#[derive(Debug, Clone, Deserialize)]
pub struct BodyHeader {
    #[serde(rename = "msg_id")]
    pub id: Option<u64>,
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
        msg: JsonValue,
    },
    SendOk {
        offset: u64,
    },
    Poll {
        offsets: HashMap<String, u64>,
    },
    PollOk {
        msgs: HashMap<String, Vec<(u64, JsonValue)>>,
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
    FwdOk {
        payload: ForwardedResponse,
    },
    Error {
        code: u8,
        text: String,
    },
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct RequestId {
    pub incarnation: u64,
    pub seq: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
pub enum ForwardedPayload {
    Send { key: String, msg: Base64Bytes },
    Poll { offsets: HashMap<String, u64> },
    CommitOffsets { offsets: HashMap<String, u64> },
    ListCommittedOffsets { keys: Vec<String> },
}

/// Successful reply to a `fwd`, nested the same way as the request.
#[expect(
    clippy::enum_variant_names,
    reason = "variants are named after their wire types `*_ok`"
)]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
pub enum ForwardedResponse {
    SendOk {
        offset: u64,
    },
    PollOk {
        msgs: HashMap<String, (u64, Vec<Base64Bytes>)>,
    },
    CommitOffsetsOk,
    ListCommittedOffsetsOk {
        offsets: HashMap<String, u64>,
    },
}
