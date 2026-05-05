// SPDX-License-Identifier: BUSL-1.1

//! Calvin scheduler restart idempotency tests.
//!
//! Covers the WAL-recovery layer: after N epochs, a freshly-opened
//! `WalManager` reads back the correct `last_applied_epoch`, including
//! out-of-order writes, vshard isolation, and the greenfield sentinel.
//!
//! End-to-end scheduler rebuild via `MultiRaft::read_committed_entries`
//! is covered by `nodedb-cluster/tests/calvin_3node_shard_failover.rs::
//! scheduler_catchup_via_raft_log_replay`.

use tempfile::TempDir;

use nodedb::control::cluster::calvin::scheduler::{NOT_YET_APPLIED_EPOCH, read_last_applied_epoch};
use nodedb::types::VShardId;
use nodedb::wal::manager::WalManager;

// ── Helper ────────────────────────────────────────────────────────────────────

fn open_wal(dir: &TempDir) -> WalManager {
    WalManager::open_for_testing(dir.path()).expect("open wal")
}

// ── Tests ─────────────────────────────────────────────────────────────────────

/// Simulating 5 epochs: append CalvinApplied records, sync, then verify that a
/// freshly-opened WalManager over the same directory reads `last_applied_epoch=5`.
#[test]
fn scheduler_restart_reads_last_applied_epoch_after_five_epochs() {
    let dir = TempDir::new().unwrap();
    let vshard_id = 3u32;

    // Simulate applying 5 epochs (epoch 1..=5, each at position 0).
    {
        let wal = open_wal(&dir);
        for epoch in 1u64..=5 {
            wal.append_calvin_applied(VShardId::new(vshard_id), epoch, 0)
                .unwrap();
        }
        // Flush to disk so the new WalManager can read them back.
        wal.sync().unwrap();
    }

    // Reopen (simulates a node restart — new WalManager, same directory).
    {
        let wal = open_wal(&dir);
        let last_epoch =
            read_last_applied_epoch(&wal, vshard_id).expect("recovery scan should succeed");
        assert_eq!(
            last_epoch, 5,
            "after five epochs, last_applied_epoch should be 5"
        );
    }
}

/// Verify that epochs applied out-of-order in the WAL are still reported
/// as the max value.  The recovery scanner must return the max, not the last
/// appended.
#[test]
fn scheduler_restart_returns_max_epoch_not_last_written() {
    let dir = TempDir::new().unwrap();
    let vshard_id = 7u32;

    {
        let wal = open_wal(&dir);
        // Write in non-monotonic order to confirm the scanner finds the max.
        wal.append_calvin_applied(VShardId::new(vshard_id), 3, 0)
            .unwrap();
        wal.append_calvin_applied(VShardId::new(vshard_id), 1, 0)
            .unwrap();
        wal.append_calvin_applied(VShardId::new(vshard_id), 5, 0)
            .unwrap();
        wal.append_calvin_applied(VShardId::new(vshard_id), 2, 0)
            .unwrap();
        wal.sync().unwrap();
    }

    {
        let wal = open_wal(&dir);
        let last_epoch = read_last_applied_epoch(&wal, vshard_id).unwrap();
        assert_eq!(
            last_epoch, 5,
            "scanner should return the max epoch (5), not the last written (2)"
        );
    }
}

/// Greenfield: a WAL with no CalvinApplied records should return the
/// `NOT_YET_APPLIED_EPOCH` sentinel (`u64::MAX`).  Epoch 0 is a valid real
/// epoch, so a distinct sentinel is required to distinguish "never applied"
/// from "applied epoch 0".  The sentinel makes `is_caught_up` trivially true
/// on a greenfield node (nothing to rebuild).
#[test]
fn scheduler_restart_greenfield_returns_sentinel() {
    let dir = TempDir::new().unwrap();
    let wal = open_wal(&dir);
    let last_epoch = read_last_applied_epoch(&wal, 1).unwrap();
    assert_eq!(
        last_epoch, NOT_YET_APPLIED_EPOCH,
        "greenfield WAL should return the not-yet-applied sentinel ({NOT_YET_APPLIED_EPOCH})"
    );
}

/// Multiple vshards: recovery for one vshard should not see epochs from another.
#[test]
fn scheduler_restart_vshard_isolation() {
    let dir = TempDir::new().unwrap();

    {
        let wal = open_wal(&dir);
        wal.append_calvin_applied(VShardId::new(1), 10, 0).unwrap();
        wal.append_calvin_applied(VShardId::new(2), 99, 0).unwrap();
        wal.append_calvin_applied(VShardId::new(1), 20, 0).unwrap();
        wal.sync().unwrap();
    }

    {
        let wal = open_wal(&dir);
        let e1 = read_last_applied_epoch(&wal, 1).unwrap();
        let e2 = read_last_applied_epoch(&wal, 2).unwrap();
        assert_eq!(e1, 20, "vshard 1 should see its own max epoch (20)");
        assert_eq!(e2, 99, "vshard 2 should see its own max epoch (99)");
    }
}
