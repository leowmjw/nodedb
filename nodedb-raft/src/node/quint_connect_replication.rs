use std::collections::HashMap;
use std::time::Duration;

use quint_connect::{Driver, Result, State, Step, quint_run, switch};
use serde::Deserialize;

use crate::message::{AppendEntriesRequest, AppendEntriesResponse, LogEntry};
use crate::node::config::RaftConfig;
use crate::node::core::RaftNode;
use crate::state::{LeaderState, NodeRole};
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
struct ModelAppendEntries {
    term: i64,
    leader_id: i64,
    prev_log_index: i64,
    prev_log_term: i64,
    entries: Vec<ModelLogEntry>,
    leader_commit: i64,
    group_id: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ModelAppendEntriesResponse {
    term: i64,
    success: bool,
    last_log_index: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ModelAppendEnvelope {
    src: i64,
    dst: i64,
    req: ModelAppendEntries,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ModelAppendResponseEnvelope {
    src: i64,
    dst: i64,
    resp: ModelAppendEntriesResponse,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ModelState {
    role1: String,
    role2: String,
    role3: String,
    current_term1: i64,
    current_term2: i64,
    current_term3: i64,
    leader_id1: i64,
    leader_id2: i64,
    leader_id3: i64,
    commit_index1: i64,
    commit_index2: i64,
    commit_index3: i64,
    last_applied1: i64,
    last_applied2: i64,
    last_applied3: i64,
    ready_commit_index1: i64,
    ready_commit_index2: i64,
    ready_commit_index3: i64,
    log1: Vec<ModelLogEntry>,
    log2: Vec<ModelLogEntry>,
    log3: Vec<ModelLogEntry>,
    next_index2: i64,
    next_index3: i64,
    match_index2: i64,
    match_index3: i64,
    append_messages: Vec<ModelAppendEnvelope>,
    append_responses: Vec<ModelAppendResponseEnvelope>,
    ready_committed1: Vec<ModelLogEntry>,
    ready_committed2: Vec<ModelLogEntry>,
    ready_committed3: Vec<ModelLogEntry>,
}

#[derive(Default)]
struct ReplicationDriver {
    nodes: HashMap<u64, RaftNode<MemStorage>>,
    append_messages: Vec<ModelAppendEnvelope>,
    append_responses: Vec<ModelAppendResponseEnvelope>,
}

impl Driver for ReplicationDriver {
    type State = ModelState;

    fn step(&mut self, step: &Step) -> Result {
        switch!(step {
            init => self.init()?,
            ClientPropose(data) => self.client_propose(data)?,
            HandleAppendEntries => self.handle_append_entries()?,
            HandleAppendEntriesResponse => self.handle_append_entries_response()?,
            TakeReady(n) => self.take_ready(n)?,
            AdvanceApplied(n) => self.advance_applied(n)?,
            Noop => (),
        })
    }
}

impl State<ReplicationDriver> for ModelState {
    fn from_driver(driver: &ReplicationDriver) -> Result<Self> {
        let n1 = driver.node(1)?;
        let n2 = driver.node(2)?;
        let n3 = driver.node(3)?;

        Ok(Self {
            role1: model_role(n1.role),
            role2: model_role(n2.role),
            role3: model_role(n3.role),
            current_term1: n1.hard_state.current_term as i64,
            current_term2: n2.hard_state.current_term as i64,
            current_term3: n3.hard_state.current_term as i64,
            leader_id1: n1.leader_id as i64,
            leader_id2: n2.leader_id as i64,
            leader_id3: n3.leader_id as i64,
            commit_index1: n1.volatile.commit_index as i64,
            commit_index2: n2.volatile.commit_index as i64,
            commit_index3: n3.volatile.commit_index as i64,
            last_applied1: n1.volatile.last_applied as i64,
            last_applied2: n2.volatile.last_applied as i64,
            last_applied3: n3.volatile.last_applied as i64,
            ready_commit_index1: n1.ready_commit_index as i64,
            ready_commit_index2: n2.ready_commit_index as i64,
            ready_commit_index3: n3.ready_commit_index as i64,
            log1: model_log(n1)?,
            log2: model_log(n2)?,
            log3: model_log(n3)?,
            next_index2: next_index(n1, 2) as i64,
            next_index3: next_index(n1, 3) as i64,
            match_index2: match_index(n1, 2) as i64,
            match_index3: match_index(n1, 3) as i64,
            append_messages: driver.append_messages.clone(),
            append_responses: driver.append_responses.clone(),
            ready_committed1: n1.ready.committed_entries.iter().map(model_entry).collect(),
            ready_committed2: n2.ready.committed_entries.iter().map(model_entry).collect(),
            ready_committed3: n3.ready.committed_entries.iter().map(model_entry).collect(),
        })
    }
}

impl ReplicationDriver {
    fn init(&mut self) -> Result {
        self.nodes.clear();
        self.append_messages.clear();
        self.append_responses.clear();

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
        leader.leader_state = Some(LeaderState::new(&[2, 3], 0));
        leader.log.append(LogEntry {
            term: 1,
            index: 1,
            data: Vec::new(),
        })?;
        leader.replicate_to_all();
        self.drain_append_messages(1);

        Ok(())
    }

    fn client_propose(&mut self, data: i64) -> Result {
        self.node_mut(1)?.propose(vec![data as u8])?;
        self.drain_append_messages(1);
        Ok(())
    }

    fn handle_append_entries(&mut self) -> Result {
        let msg = self.append_messages.remove(0);
        let req = rust_append_entries(&msg.req);
        let resp = self.node_mut(msg.dst as u64)?.handle_append_entries(&req);
        self.append_responses.push(ModelAppendResponseEnvelope {
            src: msg.dst,
            dst: msg.src,
            resp: model_append_response(resp),
        });
        Ok(())
    }

    fn handle_append_entries_response(&mut self) -> Result {
        let msg = self.append_responses.remove(0);
        let resp = AppendEntriesResponse {
            term: msg.resp.term as u64,
            success: msg.resp.success,
            last_log_index: msg.resp.last_log_index as u64,
        };
        self.node_mut(msg.dst as u64)?
            .handle_append_entries_response(msg.src as u64, &resp);
        self.drain_append_messages(msg.dst as u64);
        Ok(())
    }

    fn take_ready(&mut self, n: i64) -> Result {
        let ready = self.node_mut(n as u64)?.take_ready();
        if let Some(hard_state) = ready.hard_state {
            self.node_mut(n as u64)?
                .log
                .storage_mut()
                .save_hard_state(&hard_state)?;
        }
        Ok(())
    }

    fn advance_applied(&mut self, n: i64) -> Result {
        let node = self.node_mut(n as u64)?;
        node.advance_applied(node.commit_index());
        Ok(())
    }

    fn drain_append_messages(&mut self, src: u64) {
        let Some(node) = self.nodes.get_mut(&src) else {
            return;
        };
        self.append_messages.extend(
            std::mem::take(&mut node.ready.messages)
                .into_iter()
                .map(|(dst, req)| model_append_request(src, dst, req)),
        );
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
    let peers = [1, 2, 3].into_iter().filter(|id| *id != node_id).collect();

    RaftConfig {
        node_id,
        group_id: 1,
        peers,
        learners: vec![],
        starts_as_learner: false,
        election_timeout_min: Duration::from_millis(150),
        election_timeout_max: Duration::from_millis(300),
        heartbeat_interval: Duration::from_millis(50),
    }
}

fn model_log(node: &RaftNode<MemStorage>) -> Result<Vec<ModelLogEntry>> {
    let last_index = node.log.last_index();
    if last_index == 0 {
        return Ok(Vec::new());
    }
    Ok(node
        .log
        .entries_range(1, last_index)?
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

fn rust_entry(entry: &ModelLogEntry) -> LogEntry {
    LogEntry {
        term: entry.term as u64,
        index: entry.index as u64,
        data: if entry.data == 0 {
            Vec::new()
        } else {
            vec![entry.data as u8]
        },
    }
}

fn model_append_request(src: u64, dst: u64, req: AppendEntriesRequest) -> ModelAppendEnvelope {
    ModelAppendEnvelope {
        src: src as i64,
        dst: dst as i64,
        req: ModelAppendEntries {
            term: req.term as i64,
            leader_id: req.leader_id as i64,
            prev_log_index: req.prev_log_index as i64,
            prev_log_term: req.prev_log_term as i64,
            entries: req.entries.iter().map(model_entry).collect(),
            leader_commit: req.leader_commit as i64,
            group_id: req.group_id as i64,
        },
    }
}

fn rust_append_entries(req: &ModelAppendEntries) -> AppendEntriesRequest {
    AppendEntriesRequest {
        term: req.term as u64,
        leader_id: req.leader_id as u64,
        prev_log_index: req.prev_log_index as u64,
        prev_log_term: req.prev_log_term as u64,
        entries: req.entries.iter().map(rust_entry).collect(),
        leader_commit: req.leader_commit as u64,
        group_id: req.group_id as u64,
    }
}

fn model_append_response(resp: AppendEntriesResponse) -> ModelAppendEntriesResponse {
    ModelAppendEntriesResponse {
        term: resp.term as i64,
        success: resp.success,
        last_log_index: resp.last_log_index as i64,
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

#[quint_run(
    spec = "../quint/raft/SingleGroupReplication.qnt",
    max_steps = 20,
    max_samples = 100
)]
fn append_entries_replication_matches_quint() -> impl Driver {
    ReplicationDriver::default()
}
