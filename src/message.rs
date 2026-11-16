use std::collections::HashMap;

mod error_code;
mod incoming;
mod outgoing;

pub use incoming::{DropReason, Incoming};
pub use outgoing::Outgoing;

/// Wrappers of the same layout reuse the allocations of the inner vectors.
fn map_msgs<A, B>(
    msgs: HashMap<String, (u64, Vec<A>)>,
    convert: impl Fn(A) -> B,
) -> HashMap<String, (u64, Vec<B>)> {
    msgs.into_iter()
        .map(|(key, (from, entries))| (key, (from, entries.into_iter().map(&convert).collect())))
        .collect()
}
