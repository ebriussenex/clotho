use std::{collections::HashMap, sync::Arc};

use thiserror::Error;

#[derive(Debug, Error)]
pub enum StorageErr {
    #[error("invalid offset requested: current {current}, requested: {requested}")]
    InvalidOffset { current: usize, requested: usize },
}

#[derive(Clone)]
pub struct Logs {
    logs: Arc<tokio::sync::RwLock<HashMap<String, Vec<Option<u32>>>>>,
}

impl Logs {
    pub fn new() -> Self {
        Self {
            logs: Arc::new(tokio::sync::RwLock::new(HashMap::new())),
        }
    }

    pub async fn append(&mut self, key: String, msg: u32, offset: usize) -> Result<(), StorageErr> {
        let mut logs = self.logs.write().await;
        let log = logs.entry(key).or_insert(vec![]);

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

    pub async fn last_offset(&self, key: &str) -> Option<usize> {
        self.logs.read().await.get(key).map(|msgs| msgs.len() - 1)
    }

    pub async fn poll(
        &self,
        offsets: HashMap<String, usize>,
        limit: usize,
    ) -> HashMap<String, Vec<(usize, u32)>> {
        let logs = self.logs.read().await;

        offsets
            .into_iter()
            .map(|(key, offset)| {
                let msgs = logs
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
}
