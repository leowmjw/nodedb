# Scenario Setup

This runbook is for exercising the implementation against the shape of the
Quint models with a real local NodeDB cluster. It assumes:

- `nodedb` is already available on `PATH`.
- `mise` is installed.
- `overmind` is available through `mise`.
- Scenario files live under `quint/scenarios/scenario01/`.

The goal is to keep scenario startup reproducible and boring: use mise tasks to
generate config, then use Overmind to supervise the nodes.

## Files To Add

Add a repo-local mise file if one does not already exist:

```toml
# mise.toml
[tools]
overmind = "latest"

[tasks."scenario01:write"]
description = "Write local three-node cluster config and Procfile"
run = '''
set -eu

ROOT="${MISE_PROJECT_ROOT:-$PWD}"
SCENARIO="$ROOT/quint/scenarios/scenario01"
mkdir -p "$SCENARIO"/{node1,node2,node3,logs}

cat > "$SCENARIO/node1.toml" <<'TOML'
[server]
host = "127.0.0.1"
data_dir = "quint/scenarios/scenario01/node1/data"
memory_limit = "512MiB"
data_plane_cores = 1
log_format = "text"

[server.ports]
pgwire = 6541
native = 6551
http = 6561

[cluster]
node_id = 1
listen = "127.0.0.1:9401"
seed_nodes = ["127.0.0.1:9401", "127.0.0.1:9402", "127.0.0.1:9403"]
num_groups = 2
replication_factor = 3
force_bootstrap = true
insecure_transport = true
TOML

cat > "$SCENARIO/node2.toml" <<'TOML'
[server]
host = "127.0.0.1"
data_dir = "quint/scenarios/scenario01/node2/data"
memory_limit = "512MiB"
data_plane_cores = 1
log_format = "text"

[server.ports]
pgwire = 6542
native = 6552
http = 6562

[cluster]
node_id = 2
listen = "127.0.0.1:9402"
seed_nodes = ["127.0.0.1:9401", "127.0.0.1:9402", "127.0.0.1:9403"]
num_groups = 2
replication_factor = 3
insecure_transport = true
TOML

cat > "$SCENARIO/node3.toml" <<'TOML'
[server]
host = "127.0.0.1"
data_dir = "quint/scenarios/scenario01/node3/data"
memory_limit = "512MiB"
data_plane_cores = 1
log_format = "text"

[server.ports]
pgwire = 6543
native = 6553
http = 6563

[cluster]
node_id = 3
listen = "127.0.0.1:9403"
seed_nodes = ["127.0.0.1:9401", "127.0.0.1:9402", "127.0.0.1:9403"]
num_groups = 2
replication_factor = 3
insecure_transport = true
TOML

cat > "$ROOT/quint/Procfile.scenario01" <<'PROCFILE'
node1: nodedb --config quint/scenarios/scenario01/node1.toml
node2: nodedb --config quint/scenarios/scenario01/node2.toml
node3: nodedb --config quint/scenarios/scenario01/node3.toml
PROCFILE
'''

[tasks."scenario01:clean"]
description = "Remove scenario01 data and generated files"
run = "rm -rf quint/scenarios/scenario01 quint/Procfile.scenario01"

[tasks."scenario01:up"]
description = "Start scenario01 under overmind"
depends = ["scenario01:write"]
run = "overmind start -f quint/Procfile.scenario01"

[tasks."scenario01:restart"]
description = "Clean and start scenario01"
depends = ["scenario01:clean", "scenario01:up"]

[tasks."scenario01:check"]
description = "Check health endpoints for all three local nodes"
run = '''
set -eu
curl -fsS http://127.0.0.1:6561/health/ready
curl -fsS http://127.0.0.1:6562/health/ready
curl -fsS http://127.0.0.1:6563/health/ready
'''
```

If this repo later gets a root `mise.toml`, merge these tasks into it instead
of keeping a second task file.

## Daily Commands

Install/use Overmind through mise:

```sh
mise install
mise exec -- overmind --version
```

Generate the local scenario files:

```sh
mise run scenario01:write
```

Start the cluster:

```sh
mise run scenario01:up
```

Start from a clean data directory:

```sh
mise run scenario01:restart
```

Check readiness from another shell:

```sh
mise run scenario01:check
```

Stop the cluster from the Overmind terminal with `Ctrl-C`. If using Overmind
through a socket/server workflow later, prefer an explicit mise task such as
`scenario01:stop` that wraps `overmind quit`.

## Scenario Shape

Scenario 01 is a local three-node cluster:

| Node | Cluster RPC | PGWire | Native | HTTP |
| ---- | ----------- | ------ | ------ | ---- |
| 1    | `9401`      | `6541` | `6551` | `6561` |
| 2    | `9402`      | `6542` | `6552` | `6562` |
| 3    | `9403`      | `6543` | `6553` | `6563` |

The first node has `force_bootstrap = true` so the cluster can be created
deterministically for local modeling runs. All nodes use
`insecure_transport = true`; this is only for loopback scenarios.

Keep `num_groups = 2` and `replication_factor = 3` for early model-validation
work. Increase group count only after the core single-group and learner models
are stable.

## What To Observe

Use this scenario to compare implementation behavior against the model layers:

- Election: one leader per term per group.
- AppendEntries replication: writes accepted by a group leader should replicate
  to a quorum before commit and then flow through apply once per node.
- Join/bootstrap: nodes 2 and 3 should join through the seed list without
  being counted before they are voters.
- Ready/apply behavior: committed entries should be applied once per node.
- Restart later: after adding persistence properties to the model, restart one
  process and check that term, vote, and log state survive.

Keep model checks separate from scenario checks:

```sh
quint run quint/raft/SingleGroupElection.qnt \
  --max-samples 500 \
  --max-steps 20 \
  --invariants electionSafety termMonotonic oneVotePerTerm \
    leaderAppendsNoopInElectionTerm candidatesVoteForThemselves

cargo test -p nodedb-raft election_matches_quint -- --nocapture

quint run quint/raft/SingleGroupReplication.qnt \
  --max-samples 500 \
  --max-steps 30 \
  --invariants logMatching commitWithinLog appliedWithinCommit commitMonotonic \
    leaderCommitsOnlyCurrentTermEntries followerCommittedPrefixesMatchLeader stateMachineSafety

cargo test -p nodedb-raft append_entries_replication_matches_quint -- --nocapture

quint run quint/raft/LeaderCompleteness.qnt \
  --max-samples 200 \
  --max-steps 18 \
  --invariants electionSafety committedEntriesAgree leaderCompleteness \
    candidatesVoteForThemselves leaderAppendsNoopInElectionTerm staleCandidateDenied

cargo test -p nodedb-raft leader_completeness_matches_quint -- --nocapture

quint run quint/raft/Learners.qnt \
  --max-samples 500 \
  --max-steps 24 \
  --invariants learnerExcludedFromQuorum learnerCannotLead learnerDoesNotVote learnerAckDoesNotCommit promotionOnlyAfterCatchUp logMatching

cargo test -p nodedb-raft learners_and_membership_matches_quint -- --nocapture

quint run quint/raft/MembershipChanges.qnt \
  --max-samples 500 \
  --max-steps 12 \
  --invariants quorumMatchesVoters clusterSizeMatchesVoters trackedPeersMatchMembership \
    untrackedPeersResetProgress newPeersStartAtLogEnd heartbeatTargetsMatchTrackedPeers

cargo test -p nodedb-raft membership_changes_match_quint -- --nocapture

quint run quint/raft/MultiRaft.qnt \
  --max-samples 500 \
  --max-steps 12 \
  --invariants groupCountMatchesMounted routingLayoutStable mountedStatusesCoherent \
    learnerGroupDoesNotLead proposalOutcomeMatchesRoute readyGroupsAreMounted

cargo test -p nodedb-cluster multiraft_matches_quint -- --nocapture

quint run quint/raft/Join.qnt \
  --max-samples 500 \
  --max-steps 10 \
  --invariants successfulJoinMeansLearnerEverywhere joinNeverPromotesDirectly \
    redirectDoesNotMutate idempotentDoesNotMutate conflictDoesNotMutate \
    successResponseCarriesAllGroups

cargo test -p nodedb-cluster join_matches_quint -- --nocapture

quint run quint/raft/Snapshots.qnt \
  --max-samples 500 \
  --max-steps 12 \
  --invariants snapshotNeededForLaggingPeer leaderSnapshotBoundaryValid followerSnapshotBoundaryValid followerCommitAppliedFollowSnapshot postSnapshotLogAfterBoundary

cargo test -p nodedb-raft snapshots_match_quint -- --nocapture

quint run quint/raft/RestartPersistence.qnt \
  --max-samples 500 \
  --max-steps 14 \
  --invariants restoredHardStateMatchesStorage snapshotAndLogPersisted restartedVolatileReset secondCandidateNotGranted

cargo test -p nodedb-raft restart_persistence_matches_quint -- --nocapture
```

The Overmind scenario is for end-to-end implementation behavior. The
quint-connect tests are still the precise executable contract.

For day-to-day model work, run the direct Quint command first. If it is green,
run the focused Rust connector. Use `cargo test -p nodedb-raft` before handing
off changes that touched shared Raft behavior.
