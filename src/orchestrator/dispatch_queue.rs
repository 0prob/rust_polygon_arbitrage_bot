use parking_lot::Mutex;

use crate::core::types::EvaluatedRoute;
use crate::pipeline::arena::StateArena;
use crate::pipeline::types::PoolMeta;

/// Latest profitable batch queued when dispatch is already in flight.
pub struct PendingDispatch {
    pub arena: StateArena,
    pub profitable: Vec<EvaluatedRoute>,
    pub pool_metas: Vec<PoolMeta>,
}

pub type PendingDispatchQueue = Mutex<Option<PendingDispatch>>;

pub fn queue_pending_dispatch(queue: &PendingDispatchQueue, batch: PendingDispatch) {
    *queue.lock() = Some(batch);
}

pub fn take_pending_dispatch(queue: &PendingDispatchQueue) -> Option<PendingDispatch> {
    queue.lock().take()
}
