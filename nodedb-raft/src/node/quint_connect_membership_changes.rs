use std::collections::HashMap;
use std::time::Duration;

use quint_connect::{Driver, Result, State, Step, quint_run, switch};
use serde::Deserialize;

use crate::message::LogEntry;
use crate::node::config::RaftConfig;
use crate::node::core::RaftNode;
use crate::state::{LeaderState, NodeRole};
use crate::storage::MemStorage;

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ModelState {
    role1: String,
    leader_id1: i64,
    current_term1: i64,
    commit_index1: i64,
    voter2: bool,
    voter3: bool,
    learner3: bool,
    cluster_size1: i64,
    quorum1: i64,
    tracked2: bool,
    tracked3: bool,
    next_index2: i64,
    next_index3: i64,
    match_index2: i64,
    match_index3: i64,
    heartbeat_targets: Vec<i64>,
}

#[derive(Default)]
struct MembershipChangesDriver {
    nodes: HashMap<u64, RaftNode<MemStorage>>,
    heartbeat_targets: Vec<i64>,
}

impl Driver for MembershipChangesDriver {
    type State = ModelState;

    fn step(&mut self, step: &Step) -> Result {
        switch!(step {
            init => self.init()?,
            AddPeer2 => self.add_peer(2)?,
            AddPeer3 => self.add_peer(3)?,
            AddLearner3 => self.add_learner3()?,
            RemovePeer2 => self.remove_peer(2)?,
            RemovePeer3 => self.remove_peer(3)?,
            RemoveLearner3 => self.remove_learner3()?,
            SetVoters2 => self.set_voters(vec![2])?,
            SetVoters23 => self.set_voters(vec![2, 3])?,
            SetVoters3 => self.set_voters(vec![3])?,
            TickHeartbeat => self.tick_heartbeat()?,
            Noop => (),
        })
    }
}

impl State<MembershipChangesDriver> for ModelState {
    fn from_driver(driver: &MembershipChangesDriver) -> Result<Self> {
        let leader = driver.node(1)?;
        Ok(Self {
            role1: model_role(leader.role),
            leader_id1: leader.leader_id as i64,
            current_term1: leader.hard_state.current_term as i64,
            commit_index1: leader.volatile.commit_index as i64,
            voter2: leader.voters().contains(&2),
            voter3: leader.voters().contains(&3),
            learner3: leader.learners().contains(&3),
            cluster_size1: leader.config.cluster_size() as i64,
            quorum1: leader.config.quorum() as i64,
            tracked2: tracks_peer(leader, 2),
            tracked3: tracks_peer(leader, 3),
            next_index2: next_index(leader, 2) as i64,
            next_index3: next_index(leader, 3) as i64,
            match_index2: match_index(leader, 2) as i64,
            match_index3: match_index(leader, 3) as i64,
            heartbeat_targets: driver.heartbeat_targets.clone(),
        })
    }
}

impl MembershipChangesDriver {
    fn init(&mut self) -> Result {
        self.nodes.clear();
        self.heartbeat_targets.clear();

        for node_id in [1, 2, 3] {
            let mut node = RaftNode::new(config(node_id), MemStorage::new());
            node.restore()?;
            self.nodes.insert(node_id, node);
        }

        let leader = self.node_mut(1)?;
        leader.role = NodeRole::Leader;
        leader.hard_state.current_term = 1;
        leader.hard_state.voted_for = 1;
        leader.leader_id = 1;
        leader.volatile.commit_index = 2;
        leader.ready_commit_index = 2;
        leader.leader_state = Some(LeaderState::new(&[], 2));
        leader.log.append(LogEntry {
            term: 1,
            index: 1,
            data: Vec::new(),
        })?;
        leader.log.append(LogEntry {
            term: 1,
            index: 2,
            data: vec![7],
        })?;

        let follower2 = self.node_mut(2)?;
        follower2.hard_state.current_term = 1;
        follower2.leader_id = 1;

        let follower3 = self.node_mut(3)?;
        follower3.hard_state.current_term = 1;
        follower3.leader_id = 1;

        Ok(())
    }

    fn add_peer(&mut self, peer: u64) -> Result {
        self.clear_heartbeats();
        self.node_mut(1)?.add_peer(peer);
        Ok(())
    }

    fn add_learner3(&mut self) -> Result {
        self.clear_heartbeats();
        self.node_mut(1)?.add_learner(3);
        Ok(())
    }

    fn remove_peer(&mut self, peer: u64) -> Result {
        self.clear_heartbeats();
        self.node_mut(1)?.remove_peer(peer);
        Ok(())
    }

    fn remove_learner3(&mut self) -> Result {
        self.clear_heartbeats();
        self.node_mut(1)?.remove_learner(3);
        Ok(())
    }

    fn set_voters(&mut self, peers: Vec<u64>) -> Result {
        self.clear_heartbeats();
        self.node_mut(1)?.set_voters(peers);
        Ok(())
    }

    fn tick_heartbeat(&mut self) -> Result {
        self.node_mut(1)?.replicate_to_all();
        let mut targets: Vec<i64> = std::mem::take(&mut self.node_mut(1)?.ready.messages)
            .into_iter()
            .map(|(dst, _)| dst as i64)
            .collect();
        targets.sort_unstable();
        self.heartbeat_targets = targets;
        Ok(())
    }

    fn clear_heartbeats(&mut self) {
        self.heartbeat_targets.clear();
        if let Some(node) = self.nodes.get_mut(&1) {
            node.ready.messages.clear();
        }
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
        peers: vec![],
        learners: vec![],
        starts_as_learner: false,
        election_timeout_min: Duration::from_millis(150),
        election_timeout_max: Duration::from_millis(300),
        heartbeat_interval: Duration::from_millis(50),
    }
}

fn tracks_peer(node: &RaftNode<MemStorage>, peer: u64) -> bool {
    node.leader_state
        .as_ref()
        .map(|ls| ls.peers().contains(&peer))
        .unwrap_or(false)
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

#[quint_run(
    spec = "../quint/raft/MembershipChanges.qnt",
    max_steps = 12,
    max_samples = 100
)]
fn membership_changes_match_quint() -> impl Driver {
    MembershipChangesDriver::default()
}
