use std::{
    cmp::Ordering,
    collections::{BTreeMap, HashMap, btree_map::Entry},
    sync::{Arc, Mutex, MutexGuard, PoisonError},
};

use crate::app::CommandResponse;

use super::request::{PeerRequest, RequestId};

#[derive(Default)]
pub struct ReplyCache {
    origins: Mutex<HashMap<String, OriginState>>,
}

/// What to do with an incoming peer request.
#[derive(Debug)]
pub enum Admission {
    /// First time seen
    Execute,
    /// Already executed
    Replay(Arc<CommandResponse>),
    /// Request is being executed at the moment
    InProgress,
    /// The sender doesn't await this request anymore
    Stale,
}

struct OriginState {
    incarnation: u64,
    watermark: u64,
    requests: BTreeMap<u64, Slot>,
}

enum Slot {
    InProgress,
    /// `Arc`: replaying under the lock is a refcount increment, not a deep copy.
    Done(Arc<CommandResponse>),
}

impl OriginState {
    fn new(incarnation: u64) -> Self {
        Self {
            incarnation,
            watermark: 0,
            requests: BTreeMap::new(),
        }
    }
}

impl ReplyCache {
    pub fn admit(&self, origin: &str, request: PeerRequest) -> Admission {
        let mut origins = self.lock();
        if let Some(state) = origins.get_mut(origin) {
            return state.admit(request);
        }

        // The origin key is allocated only the first time an origin is seen.
        let mut state = OriginState::new(request.id.incarnation);
        let admission = state.admit(request);
        origins.insert(origin.to_owned(), state);
        admission
    }

    /// Takes the response already wrapped in `Arc`, so the caller copies it before taking
    /// the lock, not under it.
    pub fn complete(&self, origin: &str, id: RequestId, response: Arc<CommandResponse>) {
        if let Some(slot) = self.lock().get_mut(origin).and_then(|state| state.slot(id)) {
            *slot = Slot::Done(response);
        }
    }

    pub fn abandon(&self, origin: &str, id: RequestId) {
        if let Some(state) = self.lock().get_mut(origin)
            && state.incarnation == id.incarnation
        {
            state.requests.remove(&id.seq);
        }
    }

    fn lock(&self) -> MutexGuard<'_, HashMap<String, OriginState>> {
        self.origins.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl OriginState {
    fn admit(&mut self, request: PeerRequest) -> Admission {
        let PeerRequest { id, watermark } = request;

        match id.incarnation.cmp(&self.incarnation) {
            Ordering::Less => return Admission::Stale,
            Ordering::Greater => *self = OriginState::new(id.incarnation),
            Ordering::Equal => {}
        }

        if watermark > self.watermark {
            self.watermark = watermark;
            self.requests = self.requests.split_off(&watermark);
        }

        if id.seq < self.watermark {
            return Admission::Stale;
        }

        match self.requests.entry(id.seq) {
            Entry::Vacant(entry) => {
                entry.insert(Slot::InProgress);
                Admission::Execute
            }
            Entry::Occupied(entry) => match entry.get() {
                Slot::InProgress => Admission::InProgress,
                Slot::Done(response) => Admission::Replay(Arc::clone(response)),
            },
        }
    }

    fn slot(&mut self, id: RequestId) -> Option<&mut Slot> {
        if self.incarnation != id.incarnation {
            return None;
        }
        self.requests.get_mut(&id.seq)
    }
}

#[cfg(test)]
mod tests {
    use crate::app::SendRes;

    use super::*;

    fn request(incarnation: u64, seq: u64, watermark: u64) -> PeerRequest {
        PeerRequest {
            id: RequestId { incarnation, seq },
            watermark,
        }
    }

    fn send_ok(offset: u64) -> Arc<CommandResponse> {
        Arc::new(CommandResponse::Send(SendRes { offset }))
    }

    #[test]
    fn retry_of_executed_request_replays_reply() {
        let cache = ReplyCache::default();
        let req = request(1, 5, 5);

        assert!(matches!(cache.admit("n1", req), Admission::Execute));
        cache.complete("n1", req.id, send_ok(42));

        let Admission::Replay(response) = cache.admit("n1", req) else {
            panic!("expected a replay");
        };
        assert!(matches!(
            *response,
            CommandResponse::Send(SendRes { offset: 42 })
        ));
    }

    #[test]
    fn retry_during_execution_is_dropped() {
        let cache = ReplyCache::default();
        let req = request(1, 5, 5);

        assert!(matches!(cache.admit("n1", req), Admission::Execute));
        assert!(matches!(cache.admit("n1", req), Admission::InProgress));
    }

    #[test]
    fn same_seq_from_different_origins_is_independent() {
        let cache = ReplyCache::default();
        let req = request(1, 5, 5);

        assert!(matches!(cache.admit("n1", req), Admission::Execute));
        assert!(matches!(cache.admit("n2", req), Admission::Execute));
    }

    #[test]
    fn watermark_evicts_and_rejects_older_requests() {
        let cache = ReplyCache::default();
        let old = request(1, 3, 3);

        assert!(matches!(cache.admit("n1", old), Admission::Execute));
        cache.complete("n1", old.id, send_ok(1));

        assert!(matches!(
            cache.admit("n1", request(1, 4, 4)),
            Admission::Execute
        ));

        assert!(matches!(cache.admit("n1", old), Admission::Stale));
    }

    #[test]
    fn stale_watermark_does_not_move_it_back() {
        let cache = ReplyCache::default();

        assert!(matches!(
            cache.admit("n1", request(1, 10, 10)),
            Admission::Execute
        ));

        assert!(matches!(
            cache.admit("n1", request(1, 8, 8)),
            Admission::Stale
        ));
    }

    #[test]
    fn abandoned_request_executes_again() {
        let cache = ReplyCache::default();
        let req = request(1, 5, 5);

        assert!(matches!(cache.admit("n1", req), Admission::Execute));
        cache.abandon("n1", req.id);
        assert!(matches!(cache.admit("n1", req), Admission::Execute));
    }

    #[test]
    fn new_incarnation_resets_and_old_one_is_stale() {
        let cache = ReplyCache::default();

        assert!(matches!(
            cache.admit("n1", request(1, 100, 100)),
            Admission::Execute
        ));
        assert!(matches!(
            cache.admit("n1", request(2, 0, 0)),
            Admission::Execute
        ));
        assert!(matches!(
            cache.admit("n1", request(1, 101, 100)),
            Admission::Stale
        ));
    }

    #[test]
    fn complete_after_eviction_is_ignored() {
        let cache = ReplyCache::default();
        let req = request(1, 5, 5);

        assert!(matches!(cache.admit("n1", req), Admission::Execute));
        // the origin gave up on 5 while it was executing here
        assert!(matches!(
            cache.admit("n1", request(1, 6, 6)),
            Admission::Execute
        ));
        cache.complete("n1", req.id, send_ok(1));

        assert!(matches!(cache.admit("n1", req), Admission::Stale));
    }
}
