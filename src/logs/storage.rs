use std::{collections::HashMap, fmt::Debug, sync::Arc};

use arc_swap::ArcSwap;
use dashmap::DashMap;
use thiserror::Error;
use tokio::{
    sync::{mpsc, oneshot},
    time::Interval,
};
use tokio_util::bytes::Bytes;

use crate::domain::{LogId, Offset};

#[derive(Debug)]
pub enum Op {
    Poll,
}

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
    force_flush_interval: Interval,
}

/// Logs storage. DashMap for sharding, actor for each topic
struct Store {
    partitions: DashMap<String, PartitionHandle>,
    config: StoreConfig,
}

impl Store {
    fn new(config: StoreConfig) -> Self {
        Self {
            partitions: DashMap::new(),
            config,
        }
    }

    pub async fn append(&self, topic: &str, entry: Bytes) -> u64 {
        let handle = self.get_or_create_handle(topic);
        handle.append(entry).await
    }

    fn get_or_create_handle(&self, topic: &str) -> PartitionHandle {
        if let Some(handle) = self.partitions.get(topic) {
            return handle.clone();
        }

        self.partitions
            .entry(topic.to_string())
            .or_insert_with(|| {
                PartitionHandle::new(self.config.channel_buffer, self.config.segment_size)
            })
            .clone()
    }

    pub fn poll(
        &self,
        offsets: &HashMap<String, u64>,
        limit: usize,
    ) -> HashMap<String, (u64, Vec<Bytes>)> {
        let mut result = HashMap::with_capacity(offsets.len());

        for (topic, &from_offset) in offsets {
            let Some(handle) = self.partitions.get(topic).map(|handle| handle.clone()) else {
                continue;
            };

            if let Some(entries) = handle.read(from_offset, limit) {
                result.insert(topic.clone(), (from_offset, entries));
            }
        }
        result
    }
}

// -- Actor

enum PartitionMsg {
    Append {
        entry: Bytes,
        reply_to: oneshot::Sender<u64>,
    },
}

struct PartitionActor {
    receiver: mpsc::Receiver<PartitionMsg>,
    sealed: Arc<ArcSwap<SealedSegments>>,
    active: Arc<ArcSwap<ActiveSegment>>,

    segment_size: usize,
    base_offset: usize,
}

async fn run_partition_actor(mut actor: PartitionActor) {
    while let Some(msg) = actor.receiver.recv().await {
        actor.handle_msg(msg).await;
    }
}

/// Actor for log storage. The only place where logs are modified, with `PartitionHandle` owns state
/// and makes decisions about sealing. Actor is per-topic object. Works for the same purpose for
/// which write lock would have worked - makes writer single, appending logs sequentially.
impl PartitionActor {
    fn new(
        receiver: mpsc::Receiver<PartitionMsg>,
        sealed: Arc<ArcSwap<SealedSegments>>,
        active: Arc<ArcSwap<ActiveSegment>>,
        segment_size: usize,
    ) -> Self {
        Self {
            receiver,
            sealed,
            active,
            base_offset: 0,
            segment_size,
        }
    }

    async fn handle_msg(&mut self, msg: PartitionMsg) {
        match msg {
            PartitionMsg::Append { entry, reply_to } => {
                let offset = self.append(entry);
                // write in log successfull, but we don't response and actor system fatally broken
                reply_to
                    .send(offset)
                    .expect("fatal: partition actor broken")
            }
        }
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
    active: Arc<ArcSwap<ActiveSegment>>,
    sealed: Arc<ArcSwap<SealedSegments>>,
}

impl PartitionHandle {
    pub fn new(channel_buffer: usize, segment_size: usize) -> Self {
        let (sender, receiver) = mpsc::channel(channel_buffer);
        let active = Arc::new(ArcSwap::from_pointee(ActiveSegment::default()));
        let sealed = Arc::new(ArcSwap::from_pointee(SealedSegments::default()));

        {
            let sealed = sealed.clone();
            let active = active.clone();
            let actor = PartitionActor::new(receiver, sealed, active, segment_size);
            tokio::spawn(run_partition_actor(actor));
        }

        Self {
            sender,
            active,
            sealed,
        }
    }

    async fn append(&self, entry: Bytes) -> u64 {
        let (reply_to, reply) = oneshot::channel();

        // if send fails reply.await will fail too - no need to check twice
        self.sender
            .send(PartitionMsg::Append { entry, reply_to })
            .await
            .unwrap();

        reply.await.expect("fatal: partition actor broken!")
    }

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

struct LogStore<T> {
    logs: HashMap<LogId, Vec<Option<T>>>,
}

impl<T> Default for LogStore<T> {
    fn default() -> Self {
        Self {
            logs: HashMap::new(),
        }
    }
}

#[derive(Clone)]
pub struct Logs<T> {
    inner: Arc<tokio::sync::RwLock<LogStore<T>>>,
}

impl<T> LogStore<T> {
    fn append(&mut self, key: LogId, msg: T, offset: Offset) -> Result<(), StorageErr> {
        let log = self.logs.entry(key).or_insert(vec![]);
        let offset = offset.0 as usize;

        if offset < log.len() {
            return Err(StorageErr::InvalidOffset {
                current: log.len(),
                requested: offset,
            });
        }

        if offset > log.len() {
            log.resize_with(offset, || None);
        }

        log.push(Some(msg));
        Ok(())
    }

    fn next_offset(&self, key: &LogId) -> Offset {
        Offset(self.logs.get(key).map(|log| log.len()).unwrap_or(0) as u64)
    }

    fn poll(
        &self,
        offsets: HashMap<LogId, Offset>,
        limit: usize,
    ) -> HashMap<LogId, Vec<(Offset, T)>>
    where
        T: Clone,
    {
        offsets
            .into_iter()
            .map(|(key, offset)| {
                let msgs = self
                    .logs
                    .get(&key)
                    .map(|stored| {
                        stored
                            .iter()
                            .skip(offset.0 as usize)
                            .take(limit)
                            .enumerate()
                            .map_while(|(i, msg)| {
                                msg.clone().map(|msg| (Offset(offset.0 + i as u64), msg))
                            })
                            .collect()
                    })
                    .unwrap_or_default();

                (key, msgs)
            })
            .collect()
    }

    fn merge(&mut self, other: HashMap<LogId, Vec<(Offset, Vec<T>)>>) -> Result<(), StorageErr>
    where
        T: PartialEq + Debug,
    {
        for (key, sparsed_msgs) in other.into_iter() {
            let log = self.logs.entry(key).or_default();
            for (start, msgs) in sparsed_msgs {
                let start = start.0 as usize;
                if log.len() < start + msgs.len() {
                    log.resize_with(start + msgs.len(), || None);
                }

                for (i, (slot, msg)) in log.iter_mut().skip(start).zip(msgs).enumerate() {
                    if let Some(eslot) = slot
                        && *eslot != msg
                    {
                        return Err(StorageErr::ConflictingData {
                            existing: format!("{eslot:?}"),
                            received: format!("{msg:?}"),
                            offset: i + start,
                        });
                    }
                    *slot = Some(msg);
                }
            }
        }
        Ok(())
    }

    fn digest(&self) -> HashMap<LogId, Offset> {
        self.logs
            .iter()
            .filter_map(|(key, log)| {
                if log.is_empty() {
                    return None;
                }

                let offset = log.iter().take_while(|msg| msg.is_some()).count();
                if offset == 0 {
                    None
                } else {
                    Some((key.clone(), Offset((offset - 1) as u64)))
                }
            })
            .collect()
    }

    fn diff(&self, offsets: HashMap<LogId, Offset>) -> HashMap<LogId, Vec<(Offset, Vec<T>)>>
    where
        T: Clone,
    {
        offsets
            .into_iter()
            .filter_map(|(key, offset)| {
                let log = self.logs.get(&key)?;

                let mut res = Vec::new();
                let mut cur: Option<(Offset, Vec<T>)> = None;

                for (log_offset, msg) in log.iter().enumerate().skip(offset.0 as usize + 1) {
                    match msg {
                        None => {
                            if let Some(c) = cur.take() {
                                res.push(c);
                            }
                        }
                        Some(msg) => {
                            if let Some((_, msgs)) = &mut cur {
                                msgs.push(msg.clone());
                            } else {
                                cur = Some((Offset(log_offset as u64), vec![msg.clone()]));
                            }
                        }
                    }
                }
                if let Some(c) = cur {
                    res.push(c);
                }

                (!res.is_empty()).then_some((key, res))
            })
            .collect()
    }
}

impl<T> Default for Logs<T> {
    fn default() -> Self {
        Self {
            inner: Arc::new(tokio::sync::RwLock::new(LogStore::default())),
        }
    }
}

impl<T> Logs<T> {
    pub async fn append(&self, key: LogId, msg: T, offset: Offset) -> Result<(), StorageErr> {
        self.inner.write().await.append(key, msg, offset)
    }

    pub async fn next_offset(&self, key: &LogId) -> Offset {
        self.inner.read().await.next_offset(key)
    }

    pub async fn poll(
        &self,
        offsets: HashMap<LogId, Offset>,
        limit: usize,
    ) -> HashMap<LogId, Vec<(Offset, T)>>
    where
        T: Clone,
    {
        self.inner.read().await.poll(offsets, limit)
    }

    pub async fn merge(
        &self,
        other: HashMap<LogId, Vec<(Offset, Vec<T>)>>,
    ) -> Result<(), StorageErr>
    where
        T: PartialEq + Debug,
    {
        self.inner.write().await.merge(other)
    }

    pub async fn digest(&self) -> HashMap<LogId, Offset> {
        self.inner.read().await.digest()
    }

    pub async fn diff(
        &self,
        offsets: HashMap<LogId, Offset>,
    ) -> HashMap<LogId, Vec<(Offset, Vec<T>)>>
    where
        T: Clone,
    {
        self.inner.read().await.diff(offsets)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::collections::HashMap;

    fn key(value: &str) -> LogId {
        LogId(value.to_owned())
    }

    fn offset(value: u64) -> Offset {
        Offset(value)
    }

    #[test]
    fn append_sparse() {
        let mut logs = LogStore::default();

        logs.append(key("a"), 10, offset(0)).unwrap();
        logs.append(key("a"), 20, offset(1)).unwrap();
        logs.append(key("a"), 30, offset(4)).unwrap();

        assert_eq!(
            logs.logs[&key("a")],
            vec![Some(10), Some(20), None, None, Some(30)]
        );
    }

    #[test]
    fn append_invalid_offset() {
        let mut logs = LogStore::default();

        logs.append(key("a"), 1, offset(0)).unwrap();

        assert!(matches!(
            logs.append(key("a"), 2, offset(0)),
            Err(StorageErr::InvalidOffset { .. })
        ));
    }

    #[test]
    fn poll_stops_at_gap() {
        let mut logs = LogStore::default();

        logs.logs
            .insert(key("a"), vec![Some(1), Some(2), None, Some(4)]);

        let mut offsets = HashMap::new();
        offsets.insert(key("a"), offset(0));

        let res = logs.poll(offsets, 10);

        assert_eq!(res[&key("a")], vec![(offset(0), 1), (offset(1), 2)]);
    }

    #[test]
    fn poll_limit() {
        let mut logs = LogStore::default();

        logs.logs
            .insert(key("a"), vec![Some(1), Some(2), Some(3), Some(4)]);

        let mut offsets = HashMap::new();
        offsets.insert(key("a"), offset(0));

        let res = logs.poll(offsets, 2);

        assert_eq!(res[&key("a")], vec![(offset(0), 1), (offset(1), 2)]);
    }

    #[test]
    fn merge_into_empty() {
        let mut logs = LogStore::default();

        let mut other = HashMap::new();
        other.insert(key("a"), vec![(offset(2), vec![10, 11])]);

        logs.merge(other).unwrap();

        assert_eq!(logs.logs[&key("a")], vec![None, None, Some(10), Some(11)]);
    }

    #[test]
    fn merge_fill_gap() {
        let mut logs = LogStore::default();
        logs.logs
            .insert(key("a"), vec![Some(1), None, Some(3), Some(4), None]);

        let mut other = HashMap::new();
        other.insert(
            key("a"),
            vec![(offset(1), vec![2]), (offset(4), vec![5, 6])],
        );

        logs.merge(other).unwrap();

        assert_eq!(
            logs.logs[&key("a")],
            vec![Some(1), Some(2), Some(3), Some(4), Some(5), Some(6)]
        );
    }

    #[test]
    fn digest_contiguous_prefix() {
        let mut logs = LogStore::default();

        logs.logs
            .insert(key("a"), vec![Some(1), Some(2), None, Some(4)]);
        logs.logs
            .insert(key("b"), vec![Some(1), None, None, Some(4)]);

        let digest = logs.digest();

        assert_eq!(digest[&key("a")], offset(1));
        assert_eq!(digest[&key("b")], offset(0));
    }

    #[test]
    fn diff_multiple_segments() {
        let mut logs = LogStore::default();
        logs.logs.insert(
            key("a"),
            vec![Some(1), Some(2), None, Some(4), Some(5), None, Some(7)],
        );

        let mut offsets = HashMap::new();
        offsets.insert(key("a"), offset(0));

        let diff = logs.diff(offsets);

        assert_eq!(
            diff[&key("a")],
            vec![
                (offset(1), vec![2]),
                (offset(3), vec![4, 5]),
                (offset(6), vec![7]),
            ]
        );
    }

    #[test]
    fn diff_starts_with_gap() {
        let mut logs = LogStore::default();
        logs.logs
            .insert(key("a"), vec![None, None, Some(3), Some(4)]);

        let mut offsets = HashMap::new();
        offsets.insert(key("a"), offset(0));

        let diff = logs.diff(offsets);

        assert_eq!(diff[&key("a")], vec![(offset(2), vec![3, 4])]);
    }

    #[test]
    fn diff_merge_roundtrip() {
        let mut alog = LogStore::default();
        let mut blog = LogStore::default();

        alog.logs.insert(
            key("a"),
            vec![Some(1), Some(2), None, Some(4), Some(5), None, Some(7)],
        );

        blog.logs.insert(key("a"), vec![Some(1), Some(2)]);

        let digest = blog.digest();
        assert_eq!(digest[&key("a")], offset(1));

        let diff = alog.diff(digest);
        assert_eq!(
            diff[&key("a")],
            vec![(offset(3), vec![4, 5]), (offset(6), vec![7])],
        );

        blog.merge(diff).unwrap();

        assert_eq!(alog.logs, blog.logs);
    }

    #[test]
    fn merge_detect_conflict() {
        let mut logs = LogStore::default();

        logs.logs.insert(key("a"), vec![Some(1), Some(2)]);

        let mut other = HashMap::new();
        other.insert(key("a"), vec![(offset(1), vec![42])]);

        assert!(matches!(
            logs.merge(other),
            Err(StorageErr::ConflictingData { .. })
        ));
    }
}
