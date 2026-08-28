use std::{
    collections::{HashMap, hash_map::Entry},
    sync::Arc,
    time::Duration,
};

use thiserror::Error;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::{
    domain::{LogId, Offset},
    logs::{Logs, StorageErr},
    transport::{Transport, TransportErr},
};

use super::{
    command::{LogDelta, PeerLogDigest, ReplicationCommand},
    message::ReplicationMessage,
};

type PeerLogOffsets = Arc<tokio::sync::RwLock<HashMap<String, HashMap<LogId, Offset>>>>;

#[derive(Debug, Error)]
pub enum ReplicationErr {
    #[error(transparent)]
    Storage(#[from] StorageErr),
    #[error(transparent)]
    Transport(#[from] TransportErr),
}

#[derive(Clone)]
pub struct Replication {
    logs: Logs<u32>,
    transport: Transport,
    node_ids: Arc<Vec<String>>,
    peer_log_offsets: PeerLogOffsets,
}

impl Replication {
    pub fn new(logs: Logs<u32>, transport: Transport, node_ids: Arc<Vec<String>>) -> Self {
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
        mut replication_rx: mpsc::Receiver<ReplicationCommand>,
    ) -> Result<(), ReplicationErr> {
        loop {
            let command = tokio::select! {
                biased;

                _ = token.cancelled() => return Ok(()),
                command = replication_rx.recv() => command,
            };

            match command {
                Some(command) => self.handle_command(command).await?,
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
                biased;

                _ = token.cancelled() => return Ok(()),
                _ = interval.tick() => {},
            }

            let peer_log_offsets = self.peer_log_offsets.read().await;
            for dest in self.node_ids.iter() {
                let Some(peer_offsets) = peer_log_offsets.get(dest) else {
                    continue;
                };

                let diff = self.logs.diff(peer_offsets.clone()).await;
                let message = ReplicationMessage::LogDelta(LogDelta { msgs: diff });
                self.transport.send_replication(dest, message).await?;
            }
        }
    }

    pub async fn run_digest_announcer(
        self,
        token: CancellationToken,
        interval: Duration,
    ) -> Result<(), ReplicationErr> {
        let mut interval = tokio::time::interval(interval);
        loop {
            tokio::select! {
                biased;

                _ = token.cancelled() => return Ok(()),
                _ = interval.tick() => {},
            }

            for dest in self.node_ids.iter() {
                let message = ReplicationMessage::LogDigest(self.logs.digest().await);
                self.transport.send_replication(dest, message).await?;
            }
        }
    }

    async fn handle_command(&self, command: ReplicationCommand) -> Result<(), ReplicationErr> {
        match command {
            ReplicationCommand::PeerLogDigest(digest) => self.record_peer_digest(digest).await,
            ReplicationCommand::LogDelta(delta) => self.apply_log_delta(delta).await,
        }
    }

    async fn record_peer_digest(&self, command: PeerLogDigest) -> Result<(), ReplicationErr> {
        let PeerLogDigest { peer_id, offsets } = command;

        match self.peer_log_offsets.write().await.entry(peer_id) {
            Entry::Occupied(mut e) => *e.get_mut() = offsets,
            Entry::Vacant(e) => {
                let _ = e.insert(offsets);
            }
        }
        Ok(())
    }

    async fn apply_log_delta(&self, command: LogDelta) -> Result<(), ReplicationErr> {
        self.logs.merge(command.msgs).await?;
        Ok(())
    }
}
