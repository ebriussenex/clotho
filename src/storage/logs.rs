use std::{collections::HashMap, sync::Arc};

use thiserror::Error;

#[derive(Debug, Error)]
pub enum StorageErr {
    #[error("invalid offset requested: current: {current}, requested: {requested}")]
    InvalidOffset { current: usize, requested: usize },
    #[error(
        "conflicting message received: existing: {existing}, received: {received}, offset: {offset}"
    )]
    ConflictingMessage {
        existing: u32,
        received: u32,
        offset: usize,
    },
}

#[derive(Default)]
struct LogStore {
    logs: HashMap<String, Vec<Option<u32>>>,
}

#[derive(Clone)]
pub struct Logs {
    inner: Arc<tokio::sync::RwLock<LogStore>>,
}

impl LogStore {
    fn append(&mut self, key: String, msg: u32, offset: usize) -> Result<(), StorageErr> {
        let log = self.logs.entry(key).or_insert(vec![]);

        if offset < log.len() {
            return Err(StorageErr::InvalidOffset {
                current: log.len(),
                requested: offset,
            });
        }

        if offset > log.len() {
            log.resize(offset, None);
        }

        log.push(Some(msg));
        Ok(())
    }

    fn next_offset(&self, key: &str) -> usize {
        self.logs.get(key).map(|log| log.len()).unwrap_or(0)
    }

    fn poll(
        &self,
        offsets: HashMap<String, usize>,
        limit: usize,
    ) -> HashMap<String, Vec<(usize, u32)>> {
        offsets
            .into_iter()
            .map(|(key, offset)| {
                let msgs = self
                    .logs
                    .get(&key)
                    .map(|stored| {
                        stored
                            .iter()
                            .skip(offset)
                            .take(limit)
                            .enumerate()
                            .map_while(|(i, msg)| msg.map(|msg| (offset + i, msg)))
                            .collect()
                    })
                    .unwrap_or_default();

                (key, msgs)
            })
            .collect()
    }

    fn merge(&mut self, other: HashMap<String, Vec<(usize, Vec<u32>)>>) -> Result<(), StorageErr> {
        for (key, sparsed_msgs) in other.into_iter() {
            let log = self.logs.entry(key).or_default();
            for (start, msgs) in sparsed_msgs {
                if log.len() < start + msgs.len() {
                    log.resize(start + msgs.len(), None);
                }

                for (i, (slot, msg)) in log.iter_mut().skip(start).zip(msgs).enumerate() {
                    if let Some(eslot) = slot
                        && *eslot != msg
                    {
                        return Err(StorageErr::ConflictingMessage {
                            existing: *eslot,
                            received: msg,
                            offset: i + start,
                        });
                    }
                    *slot = Some(msg);
                }
            }
        }
        Ok(())
    }

    fn digest(&self) -> HashMap<String, usize> {
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
                    Some((key.clone(), offset - 1))
                }
            })
            .collect()
    }

    fn diff(&self, offsets: HashMap<String, usize>) -> HashMap<String, Vec<(usize, Vec<u32>)>> {
        offsets
            .into_iter()
            .filter_map(|(key, offset)| {
                let log = self.logs.get(&key)?;

                let mut res = Vec::new();
                let mut cur: Option<(usize, Vec<u32>)> = None;

                for (log_offset, msg) in log.iter().enumerate().skip(offset + 1) {
                    match msg {
                        None => {
                            if let Some(c) = cur.take() {
                                res.push(c);
                            }
                        }
                        Some(msg) => {
                            if let Some((_, msgs)) = &mut cur {
                                msgs.push(*msg);
                            } else {
                                cur = Some((log_offset, vec![*msg]));
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

impl Logs {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(tokio::sync::RwLock::new(LogStore::default())),
        }
    }

    pub async fn append(&self, key: String, msg: u32, offset: usize) -> Result<(), StorageErr> {
        self.inner.write().await.append(key, msg, offset)
    }

    pub async fn next_offset(&self, key: &str) -> usize {
        self.inner.read().await.next_offset(key)
    }

    pub async fn poll(
        &self,
        offsets: HashMap<String, usize>,
        limit: usize,
    ) -> HashMap<String, Vec<(usize, u32)>> {
        self.inner.read().await.poll(offsets, limit)
    }

    pub async fn merge(
        &self,
        other: HashMap<String, Vec<(usize, Vec<u32>)>>,
    ) -> Result<(), StorageErr> {
        self.inner.write().await.merge(other)
    }

    pub async fn digest(&self) -> HashMap<String, usize> {
        self.inner.read().await.digest()
    }

    pub async fn diff(
        &self,
        offsets: HashMap<String, usize>,
    ) -> HashMap<String, Vec<(usize, Vec<u32>)>> {
        self.inner.read().await.diff(offsets)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn append_sparse() {
        let mut logs = LogStore::default();

        logs.append("a".into(), 10, 0).unwrap();
        logs.append("a".into(), 20, 1).unwrap();
        logs.append("a".into(), 30, 4).unwrap();

        assert_eq!(
            logs.logs["a"],
            vec![Some(10), Some(20), None, None, Some(30)]
        );
    }

    #[test]
    fn append_invalid_offset() {
        let mut logs = LogStore::default();

        logs.append("a".into(), 1, 0).unwrap();

        assert!(matches!(
            logs.append("a".into(), 2, 0),
            Err(StorageErr::InvalidOffset { .. })
        ));
    }

    #[test]
    fn poll_stops_at_gap() {
        let mut logs = LogStore::default();

        logs.logs
            .insert("a".into(), vec![Some(1), Some(2), None, Some(4)]);

        let mut offsets = HashMap::new();
        offsets.insert("a".into(), 0);

        let res = logs.poll(offsets, 10);

        assert_eq!(res["a"], vec![(0, 1), (1, 2)]);
    }

    #[test]
    fn poll_limit() {
        let mut logs = LogStore::default();

        logs.logs
            .insert("a".into(), vec![Some(1), Some(2), Some(3), Some(4)]);

        let mut offsets = HashMap::new();
        offsets.insert("a".into(), 0);

        let res = logs.poll(offsets, 2);

        assert_eq!(res["a"], vec![(0, 1), (1, 2)]);
    }

    #[test]
    fn merge_into_empty() {
        let mut logs = LogStore::default();

        let mut other = HashMap::new();
        other.insert("a".into(), vec![(2, vec![10, 11])]);

        logs.merge(other).unwrap();

        assert_eq!(logs.logs["a"], vec![None, None, Some(10), Some(11)]);
    }

    #[test]
    fn merge_fill_gap() {
        let mut logs = LogStore::default();
        logs.logs
            .insert("a".into(), vec![Some(1), None, Some(3), Some(4), None]);

        let mut other = HashMap::new();
        other.insert("a".into(), vec![(1, vec![2]), (4, vec![5, 6])]);

        logs.merge(other).unwrap();

        assert_eq!(
            logs.logs["a"],
            vec![Some(1), Some(2), Some(3), Some(4), Some(5), Some(6)]
        );
    }

    #[test]
    fn digest_contiguous_prefix() {
        let mut logs = LogStore::default();

        logs.logs
            .insert("a".into(), vec![Some(1), Some(2), None, Some(4)]);
        logs.logs
            .insert("b".into(), vec![Some(1), None, None, Some(4)]);

        let digest = logs.digest();

        assert_eq!(digest["a"], 1);
        assert_eq!(digest["b"], 0);
    }

    #[test]
    fn diff_multiple_segments() {
        let mut logs = LogStore::default();
        logs.logs.insert(
            "a".into(),
            vec![Some(1), Some(2), None, Some(4), Some(5), None, Some(7)],
        );

        let mut offsets = HashMap::new();
        offsets.insert("a".into(), 0);

        let diff = logs.diff(offsets);

        assert_eq!(
            diff["a"],
            vec![(1, vec![2]), (3, vec![4, 5]), (6, vec![7]),]
        );
    }

    #[test]
    fn diff_starts_with_gap() {
        let mut logs = LogStore::default();
        logs.logs
            .insert("a".into(), vec![None, None, Some(3), Some(4)]);

        let mut offsets = HashMap::new();
        offsets.insert("a".into(), 0);

        let diff = logs.diff(offsets);

        assert_eq!(diff["a"], vec![(2, vec![3, 4]),]);
    }

    #[test]
    fn diff_merge_roundtrip() {
        let mut alog = LogStore::default();
        let mut blog = LogStore::default();

        alog.logs.insert(
            "a".into(),
            vec![Some(1), Some(2), None, Some(4), Some(5), None, Some(7)],
        );

        blog.logs.insert("a".into(), vec![Some(1), Some(2)]);

        let digest = blog.digest();
        assert_eq!(digest["a"], 1);

        let diff = alog.diff(digest);
        assert_eq!(diff["a"], vec![(3_usize, vec![4, 5]), (6_usize, vec![7])],);

        blog.merge(diff).unwrap();

        assert_eq!(alog.logs, blog.logs);
    }

    #[test]
    fn merge_detect_conflict() {
        let mut logs = LogStore::default();

        logs.logs.insert("a".into(), vec![Some(1), Some(2)]);

        let mut other = HashMap::new();
        other.insert("a".into(), vec![(1, vec![42])]);

        assert!(matches!(
            logs.merge(other),
            Err(StorageErr::ConflictingMessage { .. })
        ));
    }
}
