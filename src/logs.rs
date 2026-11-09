#![allow(
    dead_code,
    reason = "replication-only storage operations are temporarily disconnected"
)]

mod storage;

pub use storage::{StorageErr, Store, StoreConfig};
