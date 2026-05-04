use std::time::Duration;

use quint_connect::{Driver, Result, State, Step, quint_run, switch};
use serde::Deserialize;

use crate::message::{LogEntry, RequestVoteRequest};
use crate::node::config::RaftConfig;
use crate::node::core::RaftNode;
use crate::state::NodeRole;
use crate::storage::{LogStorage, MemStorage};

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ModelLogEntry {
    term: i64,
    index: i64,
    data: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ModelState {
    phase: i64,
    role: String,
    current_term: i64,
    voted_for: i64,
    commit_index: i64,
    last_applied: i64,
    leader_id: i64,
    snapshot_index: i64,
    snapshot_term: i64,
    log: Vec<ModelLogEntry>,
    ready_hard_state: bool,
    stored_current_term: i64,
    stored_voted_for: i64,
    stored_snapshot_index: i64,
    stored_snapshot_term: i64,
    stored_log: Vec<ModelLogEntry>,
    vote_granted_to3: bool,
}

#[derive(Default)]
struct RestartDriver {
    node: Option<RaftNode<MemStorage>>,
    phase: i64,
    vote_granted_to3: bool,
}

impl Driver for RestartDriver {
    type State = ModelState;

    fn step(&mut self, step: &Step) -> Result {
        switch!(step {
            init => self.init()?,
            GrantVoteTo2 => self.grant_vote_to2()?,
            PersistReady => self.persist_ready()?,
            AppendEntry1 => self.append_entry(1, 7)?,
            AppendEntry2 => self.append_entry(2, 8)?,
            AppendEntry3 => self.append_entry(3, 9)?,
            ApplySnapshot => self.apply_snapshot()?,
            RestartRestore => self.restart_restore()?,
            RequestVoteFrom3SameTerm => self.request_vote_from3_same_term()?,
            Noop => (),
        })
    }
}

impl State<RestartDriver> for ModelState {
    fn from_driver(driver: &RestartDriver) -> Result<Self> {
        let node = driver.node()?;
        let storage = node.log.storage();
        let (stored_snapshot_index, stored_snapshot_term) = storage.snapshot_metadata();
        let stored_hard_state = storage.load_hard_state()?;

        Ok(Self {
            phase: driver.phase,
            role: model_role(node.role),
            current_term: node.current_term() as i64,
            voted_for: node.voted_for() as i64,
            commit_index: node.commit_index() as i64,
            last_applied: node.last_applied() as i64,
            leader_id: node.leader_id() as i64,
            snapshot_index: node.log_snapshot_index() as i64,
            snapshot_term: node.log_snapshot_term() as i64,
            log: model_runtime_log(node)?,
            ready_hard_state: node.ready.hard_state.is_some(),
            stored_current_term: stored_hard_state.current_term as i64,
            stored_voted_for: stored_hard_state.voted_for as i64,
            stored_snapshot_index: stored_snapshot_index as i64,
            stored_snapshot_term: stored_snapshot_term as i64,
            stored_log: storage
                .load_entries_after(stored_snapshot_index)?
                .iter()
                .map(model_entry)
                .collect(),
            vote_granted_to3: driver.vote_granted_to3,
        })
    }
}

impl RestartDriver {
    fn init(&mut self) -> Result {
        let mut node = RaftNode::new(config(), MemStorage::new());
        node.restore()?;
        self.node = Some(node);
        self.phase = 0;
        self.vote_granted_to3 = false;
        Ok(())
    }

    fn grant_vote_to2(&mut self) -> Result {
        let node = self.node_mut()?;
        let _ = node.handle_request_vote(&RequestVoteRequest {
            term: 4,
            candidate_id: 2,
            last_log_index: 0,
            last_log_term: 0,
            group_id: 1,
        });
        self.phase = 1;
        Ok(())
    }

    fn persist_ready(&mut self) -> Result {
        self.node_mut()?.persist_ready_hard_state()?;
        self.node_mut()?.take_ready();
        self.phase = 2;
        Ok(())
    }

    fn append_entry(&mut self, expected_index: u64, data: u8) -> Result {
        let node = self.node_mut()?;
        node.log.append(LogEntry {
            term: 4,
            index: expected_index,
            data: vec![data],
        })?;
        self.phase += 1;
        Ok(())
    }

    fn apply_snapshot(&mut self) -> Result {
        self.node_mut()?.log.apply_snapshot(2, 4);
        self.phase = 6;
        Ok(())
    }

    fn restart_restore(&mut self) -> Result {
        let storage = self.node()?.log.storage().clone();
        let mut node = RaftNode::new(config(), storage);
        node.restore()?;
        self.node = Some(node);
        self.phase = 7;
        self.vote_granted_to3 = false;
        Ok(())
    }

    fn request_vote_from3_same_term(&mut self) -> Result {
        let node = self.node_mut()?;
        let resp = node.handle_request_vote(&RequestVoteRequest {
            term: node.current_term(),
            candidate_id: 3,
            last_log_index: node.log.last_index(),
            last_log_term: node.log.last_term(),
            group_id: 1,
        });
        self.vote_granted_to3 = resp.vote_granted;
        self.phase = 8;
        Ok(())
    }

    fn node(&self) -> Result<&RaftNode<MemStorage>> {
        self.node
            .as_ref()
            .ok_or_else(|| std::io::Error::other("driver used before init").into())
    }

    fn node_mut(&mut self) -> Result<&mut RaftNode<MemStorage>> {
        self.node
            .as_mut()
            .ok_or_else(|| std::io::Error::other("driver used before init").into())
    }
}

fn config() -> RaftConfig {
    RaftConfig {
        node_id: 1,
        group_id: 1,
        peers: vec![2, 3],
        learners: vec![],
        starts_as_learner: false,
        election_timeout_min: Duration::from_millis(150),
        election_timeout_max: Duration::from_millis(300),
        heartbeat_interval: Duration::from_millis(50),
    }
}

fn model_runtime_log(node: &RaftNode<MemStorage>) -> Result<Vec<ModelLogEntry>> {
    let lo = node.log_snapshot_index() + 1;
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

fn model_role(role: NodeRole) -> String {
    match role {
        NodeRole::Follower => "Follower",
        NodeRole::Candidate => "Candidate",
        NodeRole::Leader => "Leader",
        NodeRole::Learner => "Learner",
    }
    .to_string()
}

#[quint_run(
    spec = "../quint/raft/RestartPersistence.qnt",
    max_steps = 14,
    max_samples = 100
)]
fn restart_persistence_matches_quint() -> impl Driver {
    RestartDriver::default()
}
