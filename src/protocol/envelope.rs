#[derive(Debug, Clone)]
pub struct Envelope {
    pub src: String,
    pub msg_id: Option<usize>,
}

#[derive(Debug, Clone)]
pub struct RequestContext {
    pub reply_to: String,
    pub in_reply_to: usize,
}
