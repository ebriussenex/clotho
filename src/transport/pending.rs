use std::{
    collections::{HashMap, hash_map::Entry},
    sync::Arc,
};

use tokio::sync::oneshot;

use crate::kv::{CasRes, ReadRes};

use super::TransportErr;

#[derive(Clone)]
pub struct PendingTable {
    cas: Arc<tokio::sync::Mutex<HashMap<usize, oneshot::Sender<CasRes>>>>,
    kv_read: Arc<tokio::sync::Mutex<HashMap<usize, oneshot::Sender<ReadRes>>>>,
}

impl PendingTable {
    pub fn new() -> Self {
        PendingTable {
            cas: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
            kv_read: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
        }
    }

    pub async fn add_cas(&self, msg_id: usize) -> oneshot::Receiver<CasRes> {
        let (tx, rx) = oneshot::channel();
        self.cas.lock().await.insert(msg_id, tx);
        rx
    }

    pub async fn reg_cas(&self, msg_id: usize, res: CasRes) -> Result<(), TransportErr> {
        let mut cast = self.cas.lock().await;

        if let Entry::Occupied(entry) = cast.entry(msg_id) {
            let tx = entry.remove();
            tx.send(res)
                .map_err(|_| TransportErr::chan_send("cas_tx", "cas response"))?;
        } else {
            return Err(TransportErr::NotAwaited);
        }

        Ok(())
    }

    pub async fn remove_cas(&self, msg_id: usize) {
        self.cas.lock().await.remove(&msg_id);
    }

    pub async fn add_read(&self, msg_id: usize) -> oneshot::Receiver<ReadRes> {
        let (tx, rx) = oneshot::channel();
        self.kv_read.lock().await.insert(msg_id, tx);
        rx
    }

    pub async fn reg_read(&self, msg_id: usize, res: ReadRes) -> Result<(), TransportErr> {
        let mut kv_read_m = self.kv_read.lock().await;

        if let Entry::Occupied(entry) = kv_read_m.entry(msg_id) {
            let tx = entry.remove();
            tx.send(res)
                .map_err(|_| TransportErr::chan_send("read_tx", "read response"))?;
        } else {
            return Err(TransportErr::NotAwaited);
        }

        Ok(())
    }

    pub async fn remove_read(&self, msg_id: usize) {
        self.kv_read.lock().await.remove(&msg_id);
    }
}
