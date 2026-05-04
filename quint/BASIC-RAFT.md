# Basic Raft

## Updated: May 03, 2026

No. The current Quint model covers the first happy-path core slices, but not all of nodedb-raft.

  Covered Well

  - Single-voter election/propose/commit/Ready/apply path in Core.qnt.
  - Three-node RequestVote election flow in SingleGroupElection.qnt.
  - Successful established-leader AppendEntries replication in SingleGroupReplication.qnt.
  - Rust alignment via three quint-connect drivers.
  - Fixed/covered Ready.committed_entries pending-buffer semantics with ready_commit_index.
  - Covered stale RequestVoteResponse ignoring via the election driver/model.
  - Covered learners and membership with a focused model: AddLearner,
    learner replication, learner vote/election exclusion, learner ACK
    exclusion from quorum, safe PromoteLearner, and local PromoteSelf.


Not Yet Covered

  - AppendEntries rejection and repair:
      - old-term AppendEntries rejection: nodedb-raft/src/node/rpc.rs:17
      - prev_log_index / prev_log_term mismatch rejection: nodedb-raft/src/node/rpc.rs:33
      - leader next_index backoff and retry after failed response: nodedb-raft/src/node/rpc.rs:156
      - conflict truncation in log repair: nodedb-raft/src/log.rs:101
  - Heartbeat behavior:
      - leader tick() sends empty AppendEntries/heartbeats: nodedb-raft/src/node/core.rs:192
      - heartbeat propagation of updated leader_commit to followers is not modeled.
  - Higher/lower term AppendEntries responses:
      - higher term step-down is implemented but not modeled: nodedb-raft/src/node/rpc.rs:130
      - lower-term stale AppendEntries responses are not ignored in Rust; unlike RequestVote responses,
        there is no resp.term < current_term guard before success handling. This should be modeled/tested
        as a likely next source-risk check.
  - Current-term-only commit advancement under mixed-term logs:
      - Rust enforces it: nodedb-raft/src/node/internal.rs:176
      - current model has only term-1 entries, so it does not really test old-term commit safety.
  - Snapshot path:
      - snapshot-needed output when leader log is compacted.
      - InstallSnapshot handling and snapshot boundary state: nodedb-raft/src/node/rpc.rs:191
  - Persistence/restart:
      - restore(), hard-state persistence, log restore, snapshot metadata restore.
  - Negative client behavior:
      - propose on follower returns NotLeader: nodedb-raft/src/node/core.rs:215

  Recommended Next Model Order

  1. AppendEntries rejection + conflict repair.
  2. Stale/lower-term AppendEntriesResponse check.
  3. Heartbeat commit propagation.
  4. Mixed-term log commit rule.
  5. Learners and membership. (done)
  6. Snapshots.
  7. Restart/persistence.
