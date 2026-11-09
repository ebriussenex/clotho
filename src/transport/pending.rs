use std::{
    collections::HashMap,
    sync::{Arc, Mutex, MutexGuard, PoisonError},
};

use tokio::sync::mpsc;

use crate::app::{CommandError, CommandResponse};

use super::TransportErr;

pub type Reply = Result<CommandResponse, CommandError>;

#[derive(Clone)]
pub struct PendingTable {
    replies: Arc<Mutex<HashMap<u64, mpsc::UnboundedSender<Reply>>>>,
}

impl PendingTable {
    pub fn new() -> Self {
        Self {
            replies: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn attempts(&self) -> Attempts<'_> {
        Attempts {
            table: self,
            msg_ids: Vec::new(),
        }
    }

    pub fn register(&self, msg_id: u64, reply: Reply) -> Result<(), TransportErr> {
        let Some(reply_tx) = self.lock().remove(&msg_id) else {
            return Err(TransportErr::NotAwaited);
        };

        reply_tx.send(reply).map_err(|_| TransportErr::NotAwaited)
    }

    fn add(&self, msg_id: u64, reply_tx: mpsc::UnboundedSender<Reply>) {
        self.lock().insert(msg_id, reply_tx);
    }

    fn remove(&self, msg_ids: &[u64]) {
        let mut replies = self.lock();
        for msg_id in msg_ids {
            replies.remove(msg_id);
        }
    }

    fn lock(&self) -> MutexGuard<'_, HashMap<u64, mpsc::UnboundedSender<Reply>>> {
        self.replies.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

pub struct Attempts<'a> {
    table: &'a PendingTable,
    msg_ids: Vec<u64>,
}

impl Attempts<'_> {
    pub fn add(&mut self, msg_id: u64, reply_tx: mpsc::UnboundedSender<Reply>) {
        self.table.add(msg_id, reply_tx);
        self.msg_ids.push(msg_id);
    }
}

impl Drop for Attempts<'_> {
    fn drop(&mut self) {
        self.table.remove(&self.msg_ids);
    }
}

#[cfg(test)]
mod tests {
    use crate::app::SendRes;

    use super::*;

    fn send_ok(offset: u64) -> Reply {
        Ok(CommandResponse::Send(SendRes { offset }))
    }

    #[test]
    fn reply_to_any_attempt_reaches_the_request() {
        let table = PendingTable::new();
        let (reply_tx, mut reply_rx) = mpsc::unbounded_channel();
        let mut attempts = table.attempts();
        attempts.add(1, reply_tx.clone());
        attempts.add(2, reply_tx);

        table
            .register(1, send_ok(7))
            .expect("first attempt is awaited");

        assert!(matches!(
            reply_rx.try_recv(),
            Ok(Ok(CommandResponse::Send(SendRes { offset: 7 })))
        ));
    }

    #[test]
    fn duplicate_reply_is_not_awaited() {
        let table = PendingTable::new();
        let (reply_tx, _reply_rx) = mpsc::unbounded_channel();
        let mut attempts = table.attempts();
        attempts.add(1, reply_tx);

        table
            .register(1, send_ok(7))
            .expect("first reply is awaited");
        assert!(matches!(
            table.register(1, send_ok(7)),
            Err(TransportErr::NotAwaited)
        ));
    }

    #[test]
    fn dropping_attempts_untracks_them() {
        let table = PendingTable::new();
        let (reply_tx, _reply_rx) = mpsc::unbounded_channel();
        let mut attempts = table.attempts();
        attempts.add(1, reply_tx.clone());
        attempts.add(2, reply_tx);
        drop(attempts);

        assert!(matches!(
            table.register(1, send_ok(7)),
            Err(TransportErr::NotAwaited)
        ));
        assert!(matches!(
            table.register(2, send_ok(7)),
            Err(TransportErr::NotAwaited)
        ));
    }
}
