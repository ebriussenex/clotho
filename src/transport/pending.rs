use std::{
    collections::{HashMap, hash_map::Entry},
    sync::Arc,
};

use tokio::sync::{Mutex, oneshot};

use crate::app::CommandResponse;

use super::TransportErr;

type PendingReplies = HashMap<usize, oneshot::Sender<CommandResponse>>;

#[derive(Clone)]
pub struct PendingTable {
    replies: Arc<Mutex<PendingReplies>>,
}

impl PendingTable {
    pub fn new() -> Self {
        Self {
            replies: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub async fn add(&self, msg_id: usize) -> oneshot::Receiver<CommandResponse> {
        let (tx, rx) = oneshot::channel();
        self.replies.lock().await.insert(msg_id, tx);
        rx
    }

    pub async fn register(
        &self,
        msg_id: usize,
        response: CommandResponse,
    ) -> Result<(), TransportErr> {
        let mut replies = self.replies.lock().await;
        let Entry::Occupied(entry) = replies.entry(msg_id) else {
            return Err(TransportErr::NotAwaited);
        };

        entry
            .remove()
            .send(response)
            .map_err(|_| TransportErr::chan_send::<CommandResponse>("command_reply_tx"))
    }

    pub async fn remove(&self, msg_id: usize) {
        self.replies.lock().await.remove(&msg_id);
    }
}
