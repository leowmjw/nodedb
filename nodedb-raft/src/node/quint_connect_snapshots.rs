use std::collections::HashMap;
use std::time::Duration;

use quint_connect::{Driver, Result, State, Step, quint_run, switch};
use serde::Deserialize;

use crate::message::{InstallSnapshotRequest, LogEntry};
use crate::node::config::RaftConfig;
use crate::node::core::RaftNode;
use crate::state::{LeaderState, NodeRole};
use crate::storage::MemStorage;

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ModelLogEntry {
    term: i64,
    index: i64,
    data: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ModelInstallSnapshot {
    term: i64,
    leader_id: i64,
    last_included_index: i64,
    last_included_term: i64,
    offset: i64,
    data: i64,
    done: bool,
    group_id: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ModelInstallSnapshotEnvelope {
    src: i64,
    dst: i64,
    req: ModelInstallSnapshot,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ModelState {
    role1: String,
    role2: String,
    current_term1: i64,
    current_term2: i64,
    leader_id1: i64,
    leader_id2: i64,
    commit_index1: i64,
    commit_index2: i64,
    last_applied1: i64,
    last_applied2: i64,
    snapshot_index1: i64,
    snapshot_index2: i64,
    snapshot_term1: i64,
    snapshot_term2: i64,
    log1: Vec<ModelLogEntry>,
    log2: Vec<ModelLogEntry>,
    next_index2: i64,
    match_index2: i64,
    snapshots_needed: Vec<i64>,
    snapshot_requests: Vec<ModelInstallSnapshotEnvelope>,
}

#[derive(Default)]
struct SnapshotDriver {
    nodes: HashMap<u64, RaftNode<MemStorage>>,
    snapshot_requests: Vec<ModelInstallSnapshotEnvelope>,
}

impl Driver for SnapshotDriver {
    type State = ModelState;

    fn step(&mut self, step: &Step) -> Result {
        switch!(step {
            init => self.init()?,
            RequestSnapshotNeeded => self.request_snapshot_needed()?,
            SendInstallSnapshot => self.send_install_snapshot()?,
            HandleInstallSnapshot => self.handle_install_snapshot()?,
            Noop => (),
        })
    }
}

impl State<SnapshotDriver> for ModelState {
    fn from_driver(driver: &SnapshotDriver) -> Result<Self> {
        let n1 = driver.node(1)?;
        let n2 = driver.node(2)?;

        Ok(Self {
            role1: model_role(n1.role),
            role2: model_role(n2.role),
            current_term1: n1.hard_state.current_term as i64,
            current_term2: n2.hard_state.current_term as i64,
            leader_id1: n1.leader_id as i64,
            leader_id2: n2.leader_id as i64,
            commit_index1: n1.volatile.commit_index as i64,
            commit_index2: n2.volatile.commit_index as i64,
            last_applied1: n1.volatile.last_applied as i64,
            last_applied2: n2.volatile.last_applied as i64,
            snapshot_index1: n1.log.snapshot_index() as i64,
            snapshot_index2: n2.log.snapshot_index() as i64,
            snapshot_term1: n1.log.snapshot_term() as i64,
            snapshot_term2: n2.log.snapshot_term() as i64,
            log1: model_log(n1)?,
            log2: model_log(n2)?,
            next_index2: next_index(n1, 2) as i64,
            match_index2: match_index(n1, 2) as i64,
            snapshots_needed: n1
                .ready
                .snapshots_needed
                .iter()
                .copied()
                .map(|id| id as i64)
                .collect(),
            snapshot_requests: driver.snapshot_requests.clone(),
        })
    }
}

impl SnapshotDriver {
    fn init(&mut self) -> Result {
        self.nodes.clear();
        self.snapshot_requests.clear();

        for node_id in [1, 2] {
            let mut node = RaftNode::new(config(node_id), MemStorage::new());
            node.restore()?;
            self.nodes.insert(node_id, node);
        }

        let leader = self.node_mut(1)?;
        leader.role = NodeRole::Leader;
        leader.hard_state.current_term = 2;
        leader.hard_state.voted_for = 1;
        leader.leader_id = 1;
        leader.volatile.commit_index = 3;
        leader.leader_state = Some(LeaderState::new(&[2], 3));
        leader.log.append(LogEntry {
            term: 1,
            index: 1,
            data: Vec::new(),
        })?;
        leader.log.append(LogEntry {
            term: 1,
            index: 2,
            data: vec![1],
        })?;
        leader.log.append(LogEntry {
            term: 2,
            index: 3,
            data: vec![9],
        })?;
        leader.log.apply_snapshot(2, 1);
        if let Some(leader_state) = leader.leader_state.as_mut() {
            leader_state.set_next_index(2, 1);
            leader_state.set_match_index(2, 0);
        }

        let follower = self.node_mut(2)?;
        follower.hard_state.current_term = 1;

        Ok(())
    }

    fn request_snapshot_needed(&mut self) -> Result {
        self.node_mut(1)?.replicate_to_all();
        Ok(())
    }

    fn send_install_snapshot(&mut self) -> Result {
        let (term, last_included_index, last_included_term, group_id) = {
            let leader = self.node(1)?;
            (
                leader.hard_state.current_term,
                leader.log.snapshot_index(),
                leader.log.snapshot_term(),
                leader.group_id(),
            )
        };
        let ready = self.node_mut(1)?.take_ready();
        for peer in ready.snapshots_needed {
            self.snapshot_requests.push(ModelInstallSnapshotEnvelope {
                src: 1,
                dst: peer as i64,
                req: ModelInstallSnapshot {
                    term: term as i64,
                    leader_id: 1,
                    last_included_index: last_included_index as i64,
                    last_included_term: last_included_term as i64,
                    offset: 0,
                    data: 0,
                    done: true,
                    group_id: group_id as i64,
                },
            });
        }
        Ok(())
    }

    fn handle_install_snapshot(&mut self) -> Result {
        let msg = self.snapshot_requests.remove(0);
        let req = InstallSnapshotRequest {
            term: msg.req.term as u64,
            leader_id: msg.req.leader_id as u64,
            last_included_index: msg.req.last_included_index as u64,
            last_included_term: msg.req.last_included_term as u64,
            offset: msg.req.offset as u64,
            data: if msg.req.data == 0 {
                Vec::new()
            } else {
                vec![msg.req.data as u8]
            },
            done: msg.req.done,
            group_id: msg.req.group_id as u64,
        };
        self.node_mut(msg.dst as u64)?.handle_install_snapshot(&req);
        Ok(())
    }

    fn node(&self, id: u64) -> Result<&RaftNode<MemStorage>> {
        self.nodes
            .get(&id)
            .ok_or_else(|| std::io::Error::other(format!("node {id} missing")).into())
    }

    fn node_mut(&mut self, id: u64) -> Result<&mut RaftNode<MemStorage>> {
        self.nodes
            .get_mut(&id)
            .ok_or_else(|| std::io::Error::other(format!("node {id} missing")).into())
    }
}

fn config(node_id: u64) -> RaftConfig {
    RaftConfig {
        node_id,
        group_id: 1,
        peers: if node_id == 1 { vec![2] } else { vec![1] },
        learners: vec![],
        starts_as_learner: false,
        election_timeout_min: Duration::from_millis(150),
        election_timeout_max: Duration::from_millis(300),
        heartbeat_interval: Duration::from_millis(50),
    }
}

fn model_log(node: &RaftNode<MemStorage>) -> Result<Vec<ModelLogEntry>> {
    let lo = node.log.snapshot_index() + 1;
    let hi = node.log.last_index();
    if lo > hi {
        return Ok(Vec::new());
    }
    Ok(node
        .log
        .entries_range(lo, hi)?
        .iter()
        .map(model_entry)
        .collect())
}

fn model_entry(entry: &LogEntry) -> ModelLogEntry {
    ModelLogEntry {
        term: entry.term as i64,
        index: entry.index as i64,
        data: entry.data.first().copied().unwrap_or(0) as i64,
    }
}

fn next_index(node: &RaftNode<MemStorage>, peer: u64) -> u64 {
    node.leader_state
        .as_ref()
        .map(|ls| ls.next_index_for(peer))
        .unwrap_or(1)
}

fn match_index(node: &RaftNode<MemStorage>, peer: u64) -> u64 {
    node.leader_state
        .as_ref()
        .map(|ls| ls.match_index_for(peer))
        .unwrap_or(0)
}

fn model_role(role: NodeRole) -> String {
    match role {
        NodeRole::Follower => "Follower",
        NodeRole::Candidate => "Candidate",
        NodeRole::Leader => "Leader",
        NodeRole::Learner => "Learner",
    }
    .to_string()
}

#[quint_run(spec = "../quint/raft/Snapshots.qnt", max_steps = 12, max_samples = 100)]
fn snapshots_match_quint() -> impl Driver {
    SnapshotDriver::default()
}
