use std::{
    collections::BTreeSet,
    sync::{Arc, Mutex, PoisonError},
    time::{SystemTime, UNIX_EPOCH},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RequestId {
    /// Microseconds of wall-clock on sender node start of retries; distinguishes requests from
    /// different sequences
    pub incarnation: u64,
    /// Logical request counter
    pub seq: u64,
}

/// Deduplication identity for internode messages.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PeerRequest {
    pub id: RequestId,
    /// Lowest `seq` the sender still waits for.
    pub watermark: u64,
}

pub fn new_incarnation() -> u64 {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(elapsed) => u64::try_from(elapsed.as_micros()).unwrap_or(u64::MAX),
        Err(e) => {
            tracing::warn!(error = %e, "system clock is before unix epoch, incarnation is 0");
            0
        }
    }
}

/// Allocates ids for outbound requests and tracks which of them are still awaited, to compute
/// the watermark sent along with every attempt.
pub struct OutboundRequests {
    incarnation: u64,
    state: Mutex<OutboundState>,
}

/// Set of incomplete requests (`seq`s).
struct OutboundState {
    next_seq: u64,
    in_flight: BTreeSet<u64>,
}

impl OutboundRequests {
    pub fn new(incarnation: u64) -> Self {
        Self {
            incarnation,
            state: Mutex::new(OutboundState {
                next_seq: 0,
                in_flight: BTreeSet::new(),
            }),
        }
    }

    /// Starts a new request. It stays in flight (holds the watermark). When any `InFlight` dropped
    /// `finish` method of `OutboundRequests` is called, removing sequence, causing `watermark`
    /// method to return new value, updating (increasing) the watermark.
    pub fn begin(self: &Arc<Self>) -> InFlight {
        let mut state = self.lock();
        let seq = state.next_seq;
        // u64 seqs are never exhausted in practice; saturating instead of overflowing keeps
        // this panic-free.
        state.next_seq = seq.saturating_add(1);
        state.in_flight.insert(seq);
        drop(state);

        InFlight {
            requests: Arc::clone(self),
            id: RequestId {
                incarnation: self.incarnation,
                seq,
            },
        }
    }

    /// Lowest seq still in flight, or the next seq to be allocated if nothing is in flight.
    pub fn watermark(&self) -> u64 {
        let state = self.lock();
        state.in_flight.first().copied().unwrap_or(state.next_seq)
    }

    fn finish(&self, seq: u64) {
        self.lock().in_flight.remove(&seq);
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, OutboundState> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// An outbound request in flight. Dropping it (completion, error, or a cancelled future)
/// releases its seq from the watermark.
pub struct InFlight {
    requests: Arc<OutboundRequests>,
    id: RequestId,
}

impl InFlight {
    pub fn id(&self) -> RequestId {
        self.id
    }
}

impl Drop for InFlight {
    fn drop(&mut self) {
        self.requests.finish(self.id.seq);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ids_are_unique_and_carry_incarnation() {
        let requests = Arc::new(OutboundRequests::new(7));
        let first = requests.begin();
        let second = requests.begin();

        assert_eq!(
            first.id(),
            RequestId {
                incarnation: 7,
                seq: 0
            }
        );
        assert_eq!(
            second.id(),
            RequestId {
                incarnation: 7,
                seq: 1
            }
        );
    }

    #[test]
    fn watermark_is_lowest_in_flight_seq() {
        let requests = Arc::new(OutboundRequests::new(1));
        assert_eq!(requests.watermark(), 0);

        let r0 = requests.begin();
        let r1 = requests.begin();
        let r2 = requests.begin();
        assert_eq!(requests.watermark(), 0);

        // finishing out of order: the watermark waits for the oldest request
        drop(r1);
        assert_eq!(requests.watermark(), 0);

        drop(r0);
        assert_eq!(requests.watermark(), 2);

        drop(r2);
        assert_eq!(requests.watermark(), 3);
    }
}
