use std::time::{Duration, Instant};

use quint_connect::{Driver, Result, State, Step, quint_run, switch};
use serde::Deserialize;

use crate::message::LogEntry;
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
    role: String,
    current_term: i64,
    voted_for: i64,
    commit_index: i64,
    last_applied: i64,
    leader_id: i64,
    log: Vec<ModelLogEntry>,
    ready_hard_state: bool,
    ready_committed: Vec<ModelLogEntry>,
}

#[derive(Default)]
struct SingleVoterDriver {
    node: Option<RaftNode<MemStorage>>,
}

impl Driver for SingleVoterDriver {
    type State = ModelState;

    fn step(&mut self, step: &Step) -> Result {
        switch!(step {
            init => self.init()?,
            TickElectionTimeout => self.tick_election_timeout()?,
            ClientPropose(data) => self.client_propose(data)?,
            TakeReady => self.take_ready()?,
            AdvanceApplied => self.advance_applied()?,
            Noop => (),
        })
    }
}

impl State<SingleVoterDriver> for ModelState {
    fn from_driver(driver: &SingleVoterDriver) -> Result<Self> {
        let node = driver.node()?;
        let last_index = node.log.last_index();
        let log = if last_index == 0 {
            Vec::new()
        } else {
            node.log
                .entries_range(1, last_index)?
                .iter()
                .map(model_entry)
                .collect()
        };

        Ok(Self {
            role: model_role(node.role),
            current_term: node.hard_state.current_term as i64,
            voted_for: node.hard_state.voted_for as i64,
            commit_index: node.volatile.commit_index as i64,
            last_applied: node.volatile.last_applied as i64,
            leader_id: node.leader_id as i64,
            log,
            ready_hard_state: node.ready.hard_state.is_some(),
            ready_committed: node.ready.committed_entries.iter().map(model_entry).collect(),
        })
    }
}

impl SingleVoterDriver {
    fn init(&mut self) -> Result {
        let config = RaftConfig {
            node_id: 1,
            group_id: 1,
            peers: vec![],
            learners: vec![],
            starts_as_learner: false,
            election_timeout_min: Duration::from_millis(150),
            election_timeout_max: Duration::from_millis(300),
            heartbeat_interval: Duration::from_millis(50),
        };
        let mut node = RaftNode::new(config, MemStorage::new());
        node.restore()?;
        self.node = Some(node);
        Ok(())
    }

    fn tick_election_timeout(&mut self) -> Result {
        let node = self.node_mut()?;
        node.election_deadline_override(Instant::now() - Duration::from_millis(1));
        node.tick();
        Ok(())
    }

    fn client_propose(&mut self, data: i64) -> Result {
        self.node_mut()?.propose(vec![data as u8])?;
        Ok(())
    }

    fn take_ready(&mut self) -> Result {
        let ready = self.node_mut()?.take_ready();
        if let Some(hard_state) = ready.hard_state {
            self.node_mut()?
                .log
                .storage_mut()
                .save_hard_state(&hard_state)?;
        }
        Ok(())
    }

    fn advance_applied(&mut self) -> Result {
        let commit_index = self.node()?.commit_index();
        self.node_mut()?.advance_applied(commit_index);
        Ok(())
    }

    fn node(&self) -> Result<&RaftNode<MemStorage>> {
        self.node.as_ref().ok_or_else(|| {
            std::io::Error::other("driver used before init").into()
        })
    }

    fn node_mut(&mut self) -> Result<&mut RaftNode<MemStorage>> {
        self.node.as_mut().ok_or_else(|| {
            std::io::Error::other("driver used before init").into()
        })
    }
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

#[quint_run(spec = "../quint/raft/Core.qnt", max_steps = 12, max_samples = 100)]
fn single_voter_core_matches_quint() -> impl Driver {
    SingleVoterDriver::default()
}
