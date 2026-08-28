#[derive(Clone)]
pub enum KvRequest {
    Cas(CasReq),
    Read(ReadReq),
}

#[derive(Clone)]
pub struct CasReq {
    pub key: LogId,
    pub from: Offset,
    pub to: Offset,
}

#[derive(Clone)]
pub struct ReadReq {
    pub key: LogId,
}

pub enum KvEvent {
    CasRes(CasRes),
    ReadRes(ReadRes),
}

pub enum CasRes {
    Ok(CasOkRes),
    Retry(CasRetryRes),
    Unknown(CasResUnknown),
}

pub struct CasOkRes {
    pub in_reply_to: usize,
}

pub struct CasRetryRes {
    pub in_reply_to: usize,
}

pub struct ReadRes {
    pub value: Offset,
    pub in_reply_to: usize,
}

#[derive(Debug)]
pub struct CasResUnknown {
    pub code: u8,
    pub text: String,
    pub in_reply_to: usize,
}

impl KvEvent {
    pub fn get_in_reply_to(&self) -> usize {
        match self {
            KvEvent::CasRes(cas_res) => match cas_res {
                CasRes::Ok(CasOkRes { in_reply_to }) => *in_reply_to,
                CasRes::Retry(CasRetryRes { in_reply_to }) => *in_reply_to,
                CasRes::Unknown(CasResUnknown { in_reply_to, .. }) => *in_reply_to,
            },
            KvEvent::ReadRes(ReadRes { in_reply_to, .. }) => *in_reply_to,
        }
    }
}
use crate::domain::{LogId, Offset};
