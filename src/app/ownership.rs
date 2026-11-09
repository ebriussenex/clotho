pub struct Ownership {
    local: String,
    /// Sorted, deduplicated, always contains local.
    nodes: Vec<String>,
    seed: u64,
}

impl Ownership {
    pub fn new(local: String, peers: impl IntoIterator<Item = String>, seed: u64) -> Self {
        let mut nodes: Vec<String> = peers.into_iter().collect();
        nodes.push(local.clone());
        nodes.sort_unstable();
        nodes.dedup();

        Self { local, nodes, seed }
    }

    pub fn owner(&self, key: &str) -> &str {
        let key_hash = wyhash::v1::wyhash(key.as_bytes(), self.seed);
        self.nodes
            .iter()
            .max_by_key(|node| wyhash::v1::wyhash(node.as_bytes(), key_hash))
            .map_or(self.local.as_str(), String::as_str)
    }

    pub fn is_local(&self, key: &str) -> bool {
        self.owner(key) == self.local
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SEED: u64 = 7;
    const KEYS: [&str; 6] = ["orders", "payments", "audit", "clicks", "users", ""];

    fn nodes(ids: &[&str]) -> Vec<String> {
        ids.iter().map(|id| id.to_string()).collect()
    }

    fn ownership(local: &str, peers: &[&str]) -> Ownership {
        Ownership::new(local.to_string(), nodes(peers), SEED)
    }

    #[test]
    fn all_nodes_agree_on_owner() {
        let n1 = ownership("n1", &["n2", "n3"]);
        let n2 = ownership("n2", &["n3", "n1"]);
        let n3 = ownership("n3", &["n1", "n2"]);

        for key in KEYS {
            assert_eq!(n1.owner(key), n2.owner(key));
            assert_eq!(n1.owner(key), n3.owner(key));
        }
    }

    #[test]
    fn exactly_one_node_is_local_owner() {
        let ring = [
            ownership("n1", &["n2", "n3"]),
            ownership("n2", &["n1", "n3"]),
            ownership("n3", &["n1", "n2"]),
        ];

        for key in KEYS {
            let local_owners = ring.iter().filter(|o| o.is_local(key)).count();
            assert_eq!(local_owners, 1, "key {key:?}");
        }
    }

    #[test]
    fn duplicates_and_self_in_peers_are_ignored() {
        let clean = ownership("n1", &["n2", "n3"]);
        let noisy = ownership("n1", &["n3", "n2", "n2", "n1"]);

        for key in KEYS {
            assert_eq!(clean.owner(key), noisy.owner(key));
        }
    }

    #[test]
    fn single_node_owns_everything() {
        let single = ownership("n1", &[]);
        for key in KEYS {
            assert!(single.is_local(key));
        }
    }

    #[test]
    fn adding_node_moves_keys_only_to_new_node() {
        let before = ownership("n1", &["n2", "n3"]);
        let after = ownership("n1", &["n2", "n3", "n4"]);

        for i in 0..1000 {
            let key = format!("key-{i}");
            let (old, new) = (before.owner(&key), after.owner(&key));
            assert!(old == new || new == "n4", "{key}: {old} -> {new}");
        }
    }

    #[test]
    fn removing_node_moves_only_its_keys() {
        let before = ownership("n1", &["n2", "n3"]);
        let after = ownership("n1", &["n3"]);

        for i in 0..1000 {
            let key = format!("key-{i}");
            let (old, new) = (before.owner(&key), after.owner(&key));
            assert!(old == new || old == "n2", "{key}: {old} -> {new}");
        }
    }

    // not a contract test, rather a sanity check to catch possible degenerate bug in case `owner()
    // always gives same node`
    #[test]
    fn owner_is_not_degenerate() {
        let ring = ownership("n1", &["n2", "n3"]);
        let mut counts = std::collections::HashMap::new();
        for i in 0..3000 {
            *counts
                .entry(ring.owner(&format!("key-{i}")).to_string())
                .or_insert(0) += 1;
        }

        for node in ["n1", "n2", "n3"] {
            let count = counts.get(node).copied().unwrap_or(0);
            assert!((800..=1200).contains(&count), "{node}: {count}");
        }
    }
}
