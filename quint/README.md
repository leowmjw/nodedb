# Quint Models

This directory contains executable Quint models for the NodeDB Raft protocol.
The current models cover the single-voter core, three-node election,
three-node AppendEntries replication, learner/membership behavior, and the
snapshot catch-up and restart/persistence paths. Each model can be simulated
directly and checked against Rust with `quint-connect`.

## Day-to-Day Commands

Typecheck the current model:

```sh
quint typecheck quint/raft/Core.qnt
```

Run a direct simulation and check the current invariants:

```sh
quint run quint/raft/Core.qnt \
  --max-samples 100 \
  --max-steps 12 \
  --invariants termMonotonic commitWithinLog appliedWithinCommit readyCommitWithinCommit readyCommittedIsPendingBuffer
```

Reproduce a specific direct simulation by adding the seed printed by `quint`:

```sh
quint run quint/raft/Core.qnt \
  --max-samples 1 \
  --max-steps 12 \
  --seed 0x29b40de94a3c08b3 \
  --invariants termMonotonic commitWithinLog appliedWithinCommit readyCommitWithinCommit readyCommittedIsPendingBuffer
```

Run the Rust implementation against Quint-generated traces with
`quint-connect`:

```sh
cargo test -p nodedb-raft single_voter_core_matches_quint -- --nocapture
```

Run the three-node election model directly:

```sh
quint run quint/raft/SingleGroupElection.qnt \
  --max-samples 500 \
  --max-steps 20 \
  --invariants electionSafety termMonotonic oneVotePerTerm \
    leaderAppendsNoopInElectionTerm candidatesVoteForThemselves
```

Run the three-node election model against the Rust implementation:

```sh
cargo test -p nodedb-raft election_matches_quint -- --nocapture
```

Run the AppendEntries replication model directly:

```sh
quint run quint/raft/SingleGroupReplication.qnt \
  --max-samples 500 \
  --max-steps 30 \
  --invariants logMatching commitWithinLog appliedWithinCommit commitMonotonic \
    leaderCommitsOnlyCurrentTermEntries followerCommittedPrefixesMatchLeader stateMachineSafety
```

Run the AppendEntries replication model against the Rust implementation:

```sh
cargo test -p nodedb-raft append_entries_replication_matches_quint -- --nocapture
```

Run the learner/membership model directly:

```sh
quint run quint/raft/Learners.qnt \
  --max-samples 500 \
  --max-steps 24 \
  --invariants learnerExcludedFromQuorum learnerCannotLead learnerDoesNotVote learnerAckDoesNotCommit promotionOnlyAfterCatchUp logMatching
```

Run the learner/membership model against the Rust implementation:

```sh
cargo test -p nodedb-raft learners_and_membership_matches_quint -- --nocapture
```

Run the snapshot model directly:

```sh
quint run quint/raft/Snapshots.qnt \
  --max-samples 500 \
  --max-steps 12 \
  --invariants snapshotNeededForLaggingPeer leaderSnapshotBoundaryValid followerSnapshotBoundaryValid followerCommitAppliedFollowSnapshot postSnapshotLogAfterBoundary
```

Run the snapshot model against the Rust implementation:

```sh
cargo test -p nodedb-raft snapshots_match_quint -- --nocapture
```

Run the restart/persistence model directly:

```sh
quint run quint/raft/RestartPersistence.qnt \
  --max-samples 500 \
  --max-steps 14 \
  --invariants restoredHardStateMatchesStorage snapshotAndLogPersisted restartedVolatileReset secondCandidateNotGranted
```

Run the restart/persistence model against the Rust implementation:

```sh
cargo test -p nodedb-raft restart_persistence_matches_quint -- --nocapture
```

Run all Raft crate tests, including the Quint-connect tests:

```sh
cargo test -p nodedb-raft
```

## Current Files

- `raft/Core.qnt`: single-node, single-voter model and basic invariants.
- `raft/SingleGroupElection.qnt`: three-voter election model with in-flight
  `RequestVote` and `RequestVoteResponse` queues.
- `raft/SingleGroupReplication.qnt`: established three-node leader model with
  AppendEntries rejection, conflict repair, stale response filtering, follower
  responses, quorum-wide current-term-only commit advancement, heartbeat
  commit propagation, `Ready.committed_entries`, and apply advancement.
- `raft/Learners.qnt`: focused learner/membership model with `AddLearner`,
  learner replication, learner vote/election exclusion, learner ACK exclusion
  from quorum, safe `PromoteLearner`, and local `PromoteSelf`.
- `raft/Snapshots.qnt`: focused snapshot catch-up model with compacted leader
  log boundary, `snapshots_needed`, `InstallSnapshot`, and follower
  commit/applied advancement after snapshot apply.
- `raft/RestartPersistence.qnt`: focused restart model with persisted
  `currentTerm`/`votedFor`, post-snapshot log restore, and same-term
  no-double-vote after restart.
- `../nodedb-raft/src/node/quint_connect_core.rs`: Rust driver for
  `quint-connect`.
- `../nodedb-raft/src/node/quint_connect_election.rs`: Rust driver for the
  election model.
- `../nodedb-raft/src/node/quint_connect_replication.rs`: Rust driver for the
  AppendEntries replication model.
- `../nodedb-raft/src/node/quint_connect_learners.rs`: Rust driver for the
  learner/membership model.
- `../nodedb-raft/src/node/quint_connect_snapshots.rs`: Rust driver for the
  snapshot catch-up model.
- `../nodedb-raft/src/node/quint_connect_restart.rs`: Rust driver for the
  restart/persistence model.

## Workflow

1. Update the Quint model first.
2. Run `quint typecheck`.
3. Run `quint run` with the relevant invariants.
4. Update the Rust `quint-connect` driver if the model state or actions changed.
5. Run the focused `cargo test -p nodedb-raft <model_test_name> -- --nocapture`.
6. Record intentional source/model differences in `quint/AGENTS.md`.
