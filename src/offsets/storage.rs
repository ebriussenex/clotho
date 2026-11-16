use std::{collections::HashMap, sync::Arc};

use dashmap::DashMap;

#[derive(Clone, Default)]
pub struct CommittedOffsets {
    offsets: Arc<DashMap<String, u64>>,
}

impl CommittedOffsets {
    pub fn new() -> Self {
        Self::default()
    }

    /// An offset never moves back: retried and reordered commits are harmless.
    pub fn commit(&self, offsets: impl IntoIterator<Item = (String, u64)>) {
        for (key, new_offset) in offsets {
            self.offsets
                .entry(key)
                .and_modify(|offset| *offset = (*offset).max(new_offset))
                .or_insert(new_offset);
        }
    }

    /// Keys without a committed offset are left out.
    pub fn list(&self, keys: impl IntoIterator<Item = String>) -> HashMap<String, u64> {
        keys.into_iter()
            .filter_map(|key| {
                let offset = *self.offsets.get(&key)?;
                Some((key, offset))
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(key: &str) -> String {
        key.to_owned()
    }

    #[test]
    fn commit_and_list_requested_offsets() {
        let store = CommittedOffsets::new();

        store.commit([(key("a"), 10), (key("b"), 20)]);

        let listed = store.list([key("a"), key("missing")]);
        assert_eq!(listed, HashMap::from([(key("a"), 10)]));
    }

    #[test]
    fn commit_moves_offset_forward() {
        let store = CommittedOffsets::new();

        store.commit([(key("a"), 10)]);
        store.commit([(key("a"), 30)]);

        assert_eq!(store.list([key("a")]), HashMap::from([(key("a"), 30)]));
    }

    #[test]
    fn commit_never_moves_offset_back() {
        let store = CommittedOffsets::new();

        store.commit([(key("a"), 30)]);
        // e.g. a reordered or retried older commit
        store.commit([(key("a"), 10)]);

        assert_eq!(store.list([key("a")]), HashMap::from([(key("a"), 30)]));
    }

    #[test]
    fn concurrent_commits_keep_the_max() {
        let store = CommittedOffsets::new();

        std::thread::scope(|scope| {
            for thread in 0..4u64 {
                let store = &store;
                scope.spawn(move || {
                    for offset in (0..1000).map(|i| i * 4 + thread) {
                        store.commit([(key("a"), offset), (key("b"), offset)]);
                        let listed = store.list([key("a")]);
                        assert!(listed[&key("a")] >= offset);
                    }
                });
            }
        });

        let expected = HashMap::from([(key("a"), 3999), (key("b"), 3999)]);
        assert_eq!(store.list([key("a"), key("b")]), expected);
    }
}
