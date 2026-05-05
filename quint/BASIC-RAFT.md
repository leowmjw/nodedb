# Basic Raft

## Updated: May 05, 2026

• Findings first:

  1. The Quint model does not fully cover dynamic voter membership in nodedb-raft.
     Evidence: Rust has set_voters, add_peer, remove_peer, and remove_learner in nodedb-raft/src/node/
     membership.rs:17, nodedb-raft/src/node/membership.rs:59, nodedb-raft/src/node/membership.rs:72,
     nodedb-raft/src/node/membership.rs:117. The Quint membership slice only has AddLearner,
     PromoteLearner, and PromoteSelf in quint/raft/Learners.qnt:195, quint/raft/Learners.qnt:445, quint/
     raft/Learners.qnt:476. So removal and direct voter-set mutation are only Rust-unit-tested, not part of
     the executable protocol contract.
  2. Snapshot coverage is partial, not complete.
     Evidence: handle_install_snapshot has lower-term rejection, higher-term stepdown, and done-gated apply
     behavior in nodedb-raft/src/node/rpc.rs:202. The Quint snapshot slice covers the compacted-peer path
     and successful apply at the boundary in quint/raft/Snapshots.qnt:103 and quint/raft/Snapshots.qnt:154,
     but it does not model stale snapshot rejection or partial/non-done chunks.
  3. Some safety properties are tested indirectly, but not stated as model invariants.
     Evidence: current invariants are election safety / one-vote-per-term in quint/raft/
     SingleGroupElection.qnt:283, replication log/state-machine safety in quint/raft/
     SingleGroupReplication.qnt:632, learner quorum exclusion in quint/raft/Learners.qnt:533, and restart
     persistence checks in quint/raft/RestartPersistence.qnt:259. There is no explicit Quint invariant for
     leader completeness or a broader “future leaders contain committed entries” property.

  Answer: no, I would not claim high confidence on complete Quint coverage of nodedb-raft.

  What we do have high confidence in:

  - Single-node Ready/apply semantics: quint/raft/Core.qnt:30
  - Three-node election and stale vote handling: quint/raft/SingleGroupElection.qnt:149
  - AppendEntries rejection/repair, stale AE responses, heartbeat commit propagation, current-term-only
    commit: quint/raft/SingleGroupReplication.qnt:343, quint/raft/SingleGroupReplication.qnt:392
  - Learner exclusion from quorum and promotion catch-up rule: quint/raft/Learners.qnt:533
  - Restart persistence of hard state and post-snapshot log restore: quint/raft/RestartPersistence.qnt:185

  Why confidence is not “complete”:

  - Membership mutation surface in Rust is larger than the modeled surface.
  - Snapshot RPC semantics are only partially modeled.
  - Some important Raft properties are checked by scenario shape, not as first-class invariants.
  - The Quint suite is still mostly focused-slice modeling, not one integrated single-group model spanning
    election, repair, snapshots, restart, and membership together.

  Practical confidence level:

  - High for the currently modeled safety-critical slices.
  - Medium overall for nodedb-raft as a whole.
  - Not yet high enough to say “complete coverage”.

  If you want to close the remaining gap, the next three additions should be:

  1. Extend Learners.qnt or add MembershipChanges.qnt for add_peer, remove_peer, remove_learner,
     set_voters.
  2. Extend Snapshots.qnt with stale snapshot rejection and done = false no-apply behavior.
  3. Add an explicit leader-completeness invariant over the election/replication model.


## Completed May 04 2026

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
