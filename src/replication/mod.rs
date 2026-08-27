mod request;

use std::{
    collections::{HashMap, hash_map::Entry},
    sync::Arc,
    time::Duration,
};

use thiserror::Error;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::{
    storage::{Logs, StorageErr},
    transport::{Transport, TransportErr},
};

pub use request::{LogDelta, PeerLogDigest, ReplicationRequest};

type PeerLogOffsets = Arc<tokio::sync::RwLock<HashMap<String, HashMap<String, usize>>>>;

#[derive(Debug, Error)]
pub enum ReplicationErr {
    #[error(transparent)]
    Storage(#[from] StorageErr),
    #[error(transparent)]
    Transport(#[from] TransportErr),
}

#[derive(Clone)]
pub struct Replication {
    logs: Logs,
    transport: Transport,
    node_ids: Arc<Vec<String>>,
    peer_log_offsets: PeerLogOffsets,
}

impl Replication {
    pub fn new(logs: Logs, transport: Transport, node_ids: Arc<Vec<String>>) -> Self {
        Self {
            logs,
            transport,
            node_ids,
            peer_log_offsets: Arc::new(tokio::sync::RwLock::new(HashMap::new())),
        }
    }

    pub async fn run_inbound(
        self,
        token: CancellationToken,
        mut replication_rx: mpsc::Receiver<ReplicationRequest>,
    ) -> Result<(), ReplicationErr> {
        loop {
            let req = tokio::select! {
                _ = token.cancelled() => return Ok(()),
                req = replication_rx.recv() => req,
            };

            match req {
                Some(req) => self.handle_request(req).await?,
                None => return Ok(()),
            }
        }
    }

    pub async fn run_delta_sender(
        self,
        token: CancellationToken,
        interval: Duration,
    ) -> Result<(), ReplicationErr> {
        let mut interval = tokio::time::interval(interval);
        loop {
            tokio::select! {
                _ = token.cancelled() => return Ok(()),
                _ = interval.tick() => {},
            }

            let peer_log_offsets = self.peer_log_offsets.read().await;
            for dest in self.node_ids.iter() {
                let Some(peer_offsets) = peer_log_offsets.get(dest) else {
                    continue;
                };

                let diff = self.logs.diff(peer_offsets.clone()).await;
                let req = ReplicationRequest::LogDelta(LogDelta { msgs: diff });
                self.transport.send_replication_request(dest, req).await?;
            }
        }
    }

    pub async fn run_digest_announcer(
        self,
        token: CancellationToken,
        interval: Duration,
        src: Arc<str>,
    ) -> Result<(), ReplicationErr> {
        let mut interval = tokio::time::interval(interval);
        loop {
            tokio::select! {
                _ = token.cancelled() => return Ok(()),
                _ = interval.tick() => {},
            }

            for dest in self.node_ids.iter() {
                let req = ReplicationRequest::PeerLogDigest(PeerLogDigest {
                    peer_id: src.to_string(),
                    offsets: self.logs.digest().await,
                });
                self.transport.send_replication_request(dest, req).await?;
            }
        }
    }

    async fn handle_request(&self, req: ReplicationRequest) -> Result<(), ReplicationErr> {
        match req {
            ReplicationRequest::PeerLogDigest(digest) => self.record_peer_digest(digest).await,
            ReplicationRequest::LogDelta(delta) => self.apply_log_delta(delta).await,
        }
    }

    async fn record_peer_digest(&self, req: PeerLogDigest) -> Result<(), ReplicationErr> {
        let PeerLogDigest { peer_id, offsets } = req;

        match self.peer_log_offsets.write().await.entry(peer_id) {
            Entry::Occupied(mut e) => *e.get_mut() = offsets,
            Entry::Vacant(e) => {
                let _ = e.insert(offsets);
            }
        }
        Ok(())
    }

    async fn apply_log_delta(&self, req: LogDelta) -> Result<(), ReplicationErr> {
        self.logs.merge(req.msgs).await?;
        Ok(())
    }
}
