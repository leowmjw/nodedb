# Quint Models

This directory contains executable Quint models for the NodeDB Raft protocol.
The current model is a small single-voter contract used both for direct Quint
simulation and for model-based testing through `quint-connect`.

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
  --invariants electionSafety termMonotonic oneVotePerTerm
```

Run the three-node election model against the Rust implementation:

```sh
cargo test -p nodedb-raft election_matches_quint -- --nocapture
```

Run all Raft crate tests, including the Quint-connect test:

```sh
cargo test -p nodedb-raft
```

## Current Files

- `raft/Core.qnt`: single-node, single-voter model and basic invariants.
- `raft/SingleGroupElection.qnt`: three-voter election model with in-flight
  `RequestVote` and `RequestVoteResponse` queues.
- `../nodedb-raft/src/node/quint_connect_core.rs`: Rust driver for
  `quint-connect`.
- `../nodedb-raft/src/node/quint_connect_election.rs`: Rust driver for the
  election model.

## Workflow

1. Update the Quint model first.
2. Run `quint typecheck`.
3. Run `quint run` with the relevant invariants.
4. Update the Rust `quint-connect` driver if the model state or actions changed.
5. Run the focused `cargo test -p nodedb-raft single_voter_core_matches_quint -- --nocapture`.
6. Record intentional source/model differences in `quint/AGENTS.md`.
