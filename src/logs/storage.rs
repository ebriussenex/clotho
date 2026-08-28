use std::{collections::HashMap, fmt::Debug, sync::Arc};

use thiserror::Error;

use crate::domain::{LogId, Offset};

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
