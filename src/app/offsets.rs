use std::{collections::HashMap, sync::Arc};

use crate::{
    app::{CommitOffsetsCommand, ListCommittedOffsetsCommand},
    domain::{LogId, Offset},
};

#[derive(Clone)]
pub struct CommitedOffsets {
    inner: Arc<tokio::sync::RwLock<CommitedOffsetsStore>>,
}

#[derive(Default)]
struct CommitedOffsetsStore {
    offsets: HashMap<LogId, Offset>,
}

impl CommitedOffsetsStore {
    fn commit(&mut self, command: &CommitOffsetsCommand) {
        command.offsets.iter().for_each(|(key, new_offset)| {
            self.offsets
                .entry(key.clone())
                .and_modify(|offset| *offset = *new_offset)
                .or_insert(*new_offset);
        });
    }

    fn list(&self, command: &ListCommittedOffsetsCommand) -> HashMap<LogId, Offset> {
        command
            .keys
            .iter()
            .filter_map(|key| self.offsets.get(key).map(|offset| (key.clone(), *offset)))
            .collect()
    }
}

impl CommitedOffsets {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(tokio::sync::RwLock::new(CommitedOffsetsStore::default())),
        }
    }

    pub async fn commit(&self, command: &CommitOffsetsCommand) {
        self.inner.write().await.commit(command)
    }

    pub async fn list(&self, command: &ListCommittedOffsetsCommand) -> HashMap<LogId, Offset> {
        self.inner.read().await.list(command)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn commit_and_list_requested_offsets() {
        let store = CommitedOffsets::new();
        let command = CommitOffsetsCommand {
            offsets: HashMap::from([
                (LogId("a".to_string()), Offset(10)),
                (LogId("b".to_string()), Offset(20)),
            ]),
        };

        store.commit(&command).await;

        let listed = store
            .list(&ListCommittedOffsetsCommand {
                keys: vec![LogId("a".to_string())],
            })
            .await;

        assert_eq!(
            listed,
            HashMap::from([(LogId("a".to_string()), Offset(10))])
        );
    }

    #[tokio::test]
    async fn commit_overwrites_existing_offset() {
        let store = CommitedOffsets::new();

        store
            .commit(&CommitOffsetsCommand {
                offsets: HashMap::from([(LogId("a".to_string()), Offset(10))]),
            })
            .await;
        store
            .commit(&CommitOffsetsCommand {
                offsets: HashMap::from([(LogId("a".to_string()), Offset(30))]),
            })
            .await;

        let listed = store
            .list(&ListCommittedOffsetsCommand {
                keys: vec![LogId("a".to_string())],
            })
            .await;

        assert_eq!(
            listed,
            HashMap::from([(LogId("a".to_string()), Offset(30))])
        );
    }
}
