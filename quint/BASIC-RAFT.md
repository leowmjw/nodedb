# Basic Raft

## Updated: May 05, 2026

No. The current Quint model covers the first happy-path core slices, but not all of nodedb-raft.

  Covered Well

  - Single-voter election/propose/commit/Ready/apply path in Core.qnt.
  - Three-node RequestVote election flow in SingleGroupElection.qnt.
  - Successful established-leader AppendEntries replication in SingleGroupReplication.qnt.
  - Rust alignment via five quint-connect drivers.
  - Fixed/covered Ready.committed_entries pending-buffer semantics with ready_commit_index.
  - Covered stale RequestVoteResponse ignoring via the election driver/model.
  - Covered learners and membership with a focused model: AddLearner,
    learner replication, learner vote/election exclusion, learner ACK
    exclusion from quorum, safe PromoteLearner, and local PromoteSelf.
  - Covered snapshot catch-up with a focused model: compacted leader boundary,
    `snapshots_needed`, `InstallSnapshot`, follower snapshot apply, and
    commit/applied advancement to the snapshot boundary.
  - Covered restart/persistence with a focused model: persisted hard state,
    snapshot metadata + post-snapshot log restore, and same-term vote
    rejection after restart.


Not Yet Covered

  - AppendEntries rejection and repair:
      - old-term AppendEntries rejection: nodedb-raft/src/node/rpc.rs:17
      - prev_log_index / prev_log_term mismatch rejection: nodedb-raft/src/node/rpc.rs:33
      - leader next_index backoff and retry after failed response: nodedb-raft/src/node/rpc.rs:156
      - conflict truncation in log repair: nodedb-raft/src/log.rs:101
  - Negative client behavior:
      - propose on follower returns NotLeader: nodedb-raft/src/node/core.rs:215

  Recommended Next Model Order

  1. AppendEntries rejection + conflict repair.
  2. Stale/lower-term AppendEntriesResponse check.
  3. Heartbeat commit propagation.
  4. Mixed-term log commit rule.
  5. Learners and membership. (done)
  6. Snapshots. (done)
  7. Restart/persistence. (done)
