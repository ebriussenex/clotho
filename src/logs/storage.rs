use std::{collections::HashMap, fmt::Debug, sync::Arc, time::Duration};

use arc_swap::ArcSwap;
use dashmap::DashMap;
use thiserror::Error;
use tokio::{
    sync::{mpsc, oneshot},
    time::error::Elapsed,
};
use tokio_util::{bytes::Bytes, sync::CancellationToken, task::TaskTracker};

#[derive(Debug, Error)]
pub enum StorageErr {
    #[error("invalid offset requested: current: {current}, requested: {requested}")]
    InvalidOffset { current: usize, requested: usize },
    #[error("merge conflict: existing: {existing}, received: {received}, offset: {offset}")]
    ConflictingData {
        existing: String,
        received: String,
        offset: usize,
    },
    #[error("sender dropped")]
    SenderDropped,
    #[error("receiver dropped")]
    ReceiverDropped,
    #[error("requested topic not found")]
    TopicNotFound,
    #[error("actor error: {0}")]
    Actor(#[from] ActorErr),
    #[error("task panicked: {0}")]
    TaskPanicked(#[from] tokio::task::JoinError),
    #[error("storage is shutting down")]
    ShuttingDown,
    #[error("internal error: partition actor died")]
    InternalError,
}

#[derive(Debug, Error)]
pub enum ActorErr {
    #[error("response receiver hang up early")]
    HangUpEarly,
    #[error("sender dropped")]
    SenderDropped,
    #[error("receiver dropped")]
    ReceiverDropped,
    #[error("actor gone")]
    ActorGone,
}

pub struct StoreConfig {
    channel_buffer: usize,
    segment_size: usize,
    grace_duration: Duration,
}

/// Logs storage. DashMap for sharding, actor for each topic. Topics created dynamically on append.
struct Store {
    partitions: DashMap<String, Partition>,
    config: StoreConfig,
    shutdown_token: CancellationToken,
    tasks: TaskTracker,
}

impl Store {
    fn new(config: StoreConfig, shutdown_token: CancellationToken) -> Self {
        Self {
            partitions: DashMap::new(),
            config,
            shutdown_token,
            tasks: TaskTracker::new(),
        }
    }

    /// Appends new entry to topic. `StorageErr::ShuttingDown` means that Storage is shutting down
    /// and no new append requests accepted.
    pub async fn append(&self, topic: &str, entry: Bytes) -> Result<u64, StorageErr> {
        if self.shutdown_token.is_cancelled() {
            return Err(StorageErr::ShuttingDown);
        }

        let handle = self.get_or_create_handle(topic);
        handle.append(entry).await
    }

    fn get_or_create_handle(&self, topic: &str) -> PartitionHandle {
        if let Some(partition) = self.partitions.get(topic) {
            return partition.handle.clone();
        }

        self.partitions
            .entry(topic.to_string())
            .or_insert_with(|| {
                Partition::new(
                    self.config.channel_buffer,
                    self.config.segment_size,
                    &self.tasks,
                    self.shutdown_token.child_token(),
                )
            })
            .handle
            .clone()
    }

    /// Polls every listed topic from specified offset with limit
    pub fn poll(
        &self,
        offsets: &HashMap<String, u64>,
        limit: usize,
    ) -> HashMap<String, (u64, Vec<Bytes>)> {
        let mut result = HashMap::with_capacity(offsets.len());

        for (topic, &from_offset) in offsets {
            let Some(reader) = self
                .partitions
                .get(topic)
                .map(|partition| partition.reader.clone())
            else {
                continue;
            };

            if let Some(entries) = reader.read(from_offset, limit) {
                result.insert(topic.clone(), (from_offset, entries));
            }
        }
        result
    }

    /// Shuts down storage gracefully, waiting for append operations to complete while preventing
    /// new appends during shutdown
    pub async fn shutdown(&self) -> Result<(), Elapsed> {
        self.tasks.close();
        tokio::time::timeout(self.config.grace_duration, self.tasks.wait()).await
    }
}

/// Partition is a view to handle (of related `PartitionActor`) to append new entries and reader -
/// to read existing entries from both active and sealed parts
struct Partition {
    handle: PartitionHandle,
    reader: PartitionReader,
}

impl Partition {
    fn new(
        channel_buffer: usize,
        segment_size: usize,
        tasks: &TaskTracker,
        shutdown_token: CancellationToken,
    ) -> Self {
        Self {
            handle: PartitionHandle::new(channel_buffer, segment_size, tasks, shutdown_token),
            reader: PartitionReader::default(),
        }
    }
}

#[derive(Clone, Default)]
struct PartitionReader {
    active: Arc<ArcSwap<ActiveSegment>>,
    sealed: Arc<ArcSwap<SealedSegments>>,
}

impl PartitionReader {
    fn read(&self, from: u64, limit: usize) -> Option<Vec<Bytes>> {
        // order matters
        let active = self.active.load_full();
        let sealed = self.sealed.load_full();

        let end = active.base_offset + active.entries.count() as u64;
        let available = end.saturating_sub(from) as usize;
        let take = available.min(limit);
        if take == 0 {
            return None;
        }

        let mut result = Vec::with_capacity(take);
        sealed.read_into(from, active.base_offset, take, &mut result);

        let next_offset = from + result.len() as u64;
        if next_offset >= active.base_offset {
            active.read_into(next_offset, take, &mut result);
        }

        Some(result)
    }
}

// -- Actor

enum PartitionMsg {
    Append {
        entry: Bytes,
        reply_to: oneshot::Sender<u64>,
    },
}

/// Actor for log storage. The only place where logs are modified, with `PartitionHandle` owns state
/// and makes decisions about sealing. Actor is per-topic object. Works for the same purpose for
/// which write lock would have worked - makes writer single, appending logs sequentially.
struct PartitionActor {
    receiver: mpsc::Receiver<PartitionMsg>,
    sealed: Arc<ArcSwap<SealedSegments>>,
    active: Arc<ArcSwap<ActiveSegment>>,

    segment_size: usize,
    base_offset: usize,

    shutdown_token: CancellationToken,
}

async fn run_partition_actor(mut actor: PartitionActor) -> Result<(), StorageErr> {
    loop {
        tokio::select! {
            biased;
            _ = actor.shutdown_token.cancelled() => break,
            msg = actor.receiver.recv() => match msg {
                Some(msg) => actor.handle_msg(msg).await?,
                None => return Ok(()),
            },
        }
    }

    // gracefully write messages we got
    actor.receiver.close();
    while let Some(msg) = actor.receiver.recv().await {
        actor.handle_msg(msg).await?;
    }
    Ok(())
}

impl PartitionActor {
    fn new(
        receiver: mpsc::Receiver<PartitionMsg>,
        sealed: Arc<ArcSwap<SealedSegments>>,
        active: Arc<ArcSwap<ActiveSegment>>,
        segment_size: usize,
        shutdown_token: CancellationToken,
    ) -> Self {
        Self {
            receiver,
            sealed,
            active,
            base_offset: 0,
            segment_size,
            shutdown_token,
        }
    }

    async fn handle_msg(&mut self, msg: PartitionMsg) -> Result<(), StorageErr> {
        match msg {
            PartitionMsg::Append { entry, reply_to } => {
                let offset = self.append(entry);
                // write in log successfull, but we don't response and actor system fatally broken
                reply_to
                    .send(offset)
                    .map_err(|_| StorageErr::InternalError)?;
            }
        }
        Ok(())
    }

    fn append(&mut self, entry: Bytes) -> u64 {
        let active = self.active.load();
        let idx = active.entries.push(entry);
        let offset = active.base_offset + idx as u64;
        drop(active);

        if idx + 1 == self.segment_size {
            self.seal();
        }

        offset
    }

    fn seal(&mut self) {
        let old = self.active.load_full();

        let entries: Vec<Bytes> = old.entries.iter().map(|(_, b)| b.clone()).collect();

        let new_seald_segm = Arc::new(SealedSegment {
            base_offset: old.base_offset,
            entries,
        });

        // store new sealed segments
        let mut segments = self.sealed.load().segments.clone();
        segments.push(new_seald_segm);
        self.sealed.store(Arc::new(SealedSegments { segments }));

        // change active
        self.active.store(Arc::new(ActiveSegment {
            base_offset: old.base_offset + self.segment_size as u64,
            entries: boxcar::Vec::with_capacity(self.segment_size),
        }));
    }
}

/// Owner of sealed and active logs. Handle for Actor. When created spawns tokio task for
/// corresponding actor.
#[derive(Clone)]
struct PartitionHandle {
    sender: mpsc::Sender<PartitionMsg>,
}

impl PartitionHandle {
    pub fn new(
        channel_buffer: usize,
        segment_size: usize,
        tasks: &TaskTracker,
        shutdown_token: CancellationToken,
    ) -> Self {
        let (sender, receiver) = mpsc::channel(channel_buffer);
        let active = Arc::new(ArcSwap::from_pointee(ActiveSegment::default()));
        let sealed = Arc::new(ArcSwap::from_pointee(SealedSegments::default()));

        {
            let sealed = sealed.clone();
            let active = active.clone();
            let actor = PartitionActor::new(receiver, sealed, active, segment_size, shutdown_token);
            tasks.spawn(run_partition_actor(actor));
        }

        Self { sender }
    }

    async fn append(&self, entry: Bytes) -> Result<u64, StorageErr> {
        let (reply_to, reply) = oneshot::channel();

        // only valid reason to fail
        self.sender
            .send(PartitionMsg::Append { entry, reply_to })
            .await
            .map_err(|_| StorageErr::ShuttingDown)?;

        Ok(reply.await.map_err(|_| StorageErr::InternalError)?)
    }
}

// -- Segments

/// Current active segment. Sealed and drained when size of segment size.
#[derive(Default)]
struct ActiveSegment {
    entries: boxcar::Vec<Bytes>,
    base_offset: u64,
}

/// Sealed segments. Each segment immutable. New segment is added when ActiveSegment is full.
/// Sorted by base_offset.
#[derive(Default)]
struct SealedSegments {
    segments: Vec<Arc<SealedSegment>>,
}

#[derive(Default)]
struct SealedSegment {
    entries: Vec<Bytes>,
    base_offset: u64,
}

impl SealedSegments {
    /// Reads segments with offsets [from, until), not more then dst.len() == limit
    fn read_into(&self, from: u64, until: u64, limit: usize, dst: &mut Vec<Bytes>) {
        if from >= until {
            return;
        }

        if self.segments.is_empty() {
            return;
        }

        // binary search first segment idx, which base_offset > from; this idx - 1 is the first
        // segment we want read; if no such - return
        let idx = self.segments.partition_point(|s| s.base_offset <= from);
        let Some(start) = idx.checked_sub(1) else {
            return;
        };

        let mut pos = from;
        for seg in &self.segments[start..] {
            if dst.len() >= limit || pos >= until {
                break;
            }

            let local_start = (pos - seg.base_offset) as usize;
            if local_start >= seg.entries.len() {
                break; // should never happen - empty space between segments
            }

            let seg_end = seg.base_offset + seg.entries.len() as u64;
            let stop = seg_end.min(until);
            let local_end = ((stop - pos) as usize).min(limit - dst.len());

            dst.extend_from_slice(&seg.entries[local_start..local_start + local_end]);
            pos += local_end as u64;
        }
    }
}

impl ActiveSegment {
    /// Reads segments with offset. Fills dst untill reaching limit;
    fn read_into(&self, from: u64, limit: usize, dst: &mut Vec<Bytes>) {
        let mut idx = (from - self.base_offset) as usize;
        // NOTE: impossible to just copy from boxcar correctly, we iterate over it; ok since nor dst nor
        // active is too big
        while dst.len() < limit {
            match self.entries.get(idx) {
                Some(entry) => dst.push(entry.clone()),
                None => break,
            }
            idx += 1;
        }
    }
}
