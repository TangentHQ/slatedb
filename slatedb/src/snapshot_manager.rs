use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;
use uuid::Uuid;

use crate::oracle::{DbOracle, Oracle};
use crate::utils::IdGenerator;
use slatedb_common::DbRand;

/// Tracks active snapshot registrations by sequence number.
///
/// `DbSnapshot` instances register here so the flusher/compactor knows the
/// oldest seq that must be retained.
pub(crate) struct SnapshotManager {
    inner: RwLock<SnapshotManagerInner>,
    db_rand: Arc<DbRand>,
}

struct SnapshotManagerInner {
    /// Map of snapshot id to sequence number.
    active_snapshots: HashMap<Uuid, u64>,
    /// The oracle for tracking the last committed sequence number.
    oracle: Arc<DbOracle>,
}

impl SnapshotManager {
    pub(crate) fn new(oracle: Arc<DbOracle>, db_rand: Arc<DbRand>) -> Self {
        Self {
            inner: RwLock::new(SnapshotManagerInner {
                active_snapshots: HashMap::new(),
                oracle,
            }),
            db_rand,
        }
    }

    pub(crate) fn new_snapshot(&self, seq: Option<u64>) -> (Uuid, u64) {
        self.register(|oracle| seq.unwrap_or_else(|| oracle.last_committed_seq()))
    }

    pub(crate) fn new_durable_snapshot(&self) -> (Uuid, u64) {
        self.register(|oracle| {
            oracle
                .last_remote_persisted_seq()
                .min(oracle.last_committed_seq())
        })
    }

    fn register(&self, sequence: impl FnOnce(&DbOracle) -> u64) -> (Uuid, u64) {
        let snapshot_id = self.db_rand.rng().gen_uuid();
        let mut inner = self.inner.write();
        let seq = sequence(&inner.oracle);
        inner.active_snapshots.insert(snapshot_id, seq);
        (snapshot_id, seq)
    }

    /// Keep the durable frontier readable even when all active snapshots are
    /// newer. Sample it under the registration lock so a new durable snapshot
    /// cannot start behind a retention boundary already sampled by a flusher.
    pub(crate) fn min_retained_seq(&self) -> u64 {
        let inner = self.inner.read();
        let durable = inner
            .oracle
            .last_remote_persisted_seq()
            .min(inner.oracle.last_committed_seq());
        inner
            .active_snapshots
            .values()
            .copied()
            .min()
            .unwrap_or(durable)
            .min(durable)
    }

    pub(crate) fn drop_snapshot(&self, snapshot_id: &Uuid) {
        let mut inner = self.inner.write();
        let removed = inner.active_snapshots.remove(snapshot_id);
        assert!(
            removed.is_some(),
            "drop_snapshot called on snapshot_id that is not tracked"
        );
    }

    /// Oldest live view, used by snapshot lifecycle tests.
    #[cfg(test)]
    pub(crate) fn min_active_seq(&self) -> Option<u64> {
        let inner = self.inner.read();
        inner.active_snapshots.values().copied().min()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db_status::DbStatusManager;

    #[test]
    fn durable_snapshots_and_retention_keep_the_remote_frontier() {
        let oracle = Arc::new(DbOracle::new(
            20,
            20,
            10,
            Arc::new(DbStatusManager::new(10)),
        ));
        let manager = SnapshotManager::new(oracle.clone(), Arc::new(DbRand::new(0)));
        assert_eq!(manager.min_retained_seq(), 10);
        let (id, seq) = manager.new_durable_snapshot();
        assert_eq!(seq, 10);
        oracle.advance_durable_seq(20);
        assert_eq!(manager.min_retained_seq(), 10);
        manager.drop_snapshot(&id);
        assert_eq!(manager.min_retained_seq(), 20);
        let (_, seq) = manager.new_durable_snapshot();
        assert_eq!(seq, 20);
    }

    fn new_snapshot_manager(seq: u64) -> SnapshotManager {
        SnapshotManager::new(
            Arc::new(DbOracle::new(
                seq,
                seq,
                seq,
                Arc::new(DbStatusManager::new(seq)),
            )),
            Arc::new(DbRand::new(0)),
        )
    }

    #[test]
    fn test_new_snapshot_uses_oracle_seq() {
        let mgr = new_snapshot_manager(123);

        let (_, seq) = mgr.new_snapshot(None);
        assert_eq!(seq, 123);
    }

    #[test]
    fn test_new_snapshot_and_min_seq() {
        let mgr = new_snapshot_manager(0);
        assert_eq!(mgr.min_active_seq(), None);

        mgr.new_snapshot(Some(10));
        assert_eq!(mgr.min_active_seq(), Some(10));

        mgr.new_snapshot(Some(5));
        assert_eq!(mgr.min_active_seq(), Some(5));

        mgr.new_snapshot(Some(20));
        assert_eq!(mgr.min_active_seq(), Some(5));
    }

    #[test]
    fn test_drop_snapshot_removes_entry() {
        let mgr = new_snapshot_manager(0);
        let (snapshot_10, _) = mgr.new_snapshot(Some(10));
        let (snapshot_20, _) = mgr.new_snapshot(Some(20));
        assert_eq!(mgr.min_active_seq(), Some(10));

        mgr.drop_snapshot(&snapshot_10);
        assert_eq!(mgr.min_active_seq(), Some(20));

        mgr.drop_snapshot(&snapshot_20);
        assert_eq!(mgr.min_active_seq(), None);
    }

    #[test]
    fn test_multiple_snapshots_with_same_seq() {
        let mgr = new_snapshot_manager(0);
        let (snapshot_1, _) = mgr.new_snapshot(Some(10));
        let (snapshot_2, _) = mgr.new_snapshot(Some(10));
        let (snapshot_3, _) = mgr.new_snapshot(Some(10));

        mgr.drop_snapshot(&snapshot_1);
        assert_eq!(mgr.min_active_seq(), Some(10));

        mgr.drop_snapshot(&snapshot_2);
        assert_eq!(mgr.min_active_seq(), Some(10));

        mgr.drop_snapshot(&snapshot_3);
        assert_eq!(mgr.min_active_seq(), None);
    }

    #[test]
    #[should_panic]
    fn test_drop_snapshot_nonexistent_panics() {
        let mgr = new_snapshot_manager(0);
        mgr.drop_snapshot(&Uuid::nil());
    }
}
