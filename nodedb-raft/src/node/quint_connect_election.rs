use std::collections::HashMap;
use std::time::{Duration, Instant};

use quint_connect::{Driver, Result, State, Step, quint_run, switch};
use serde::Deserialize;

use crate::message::{RequestVoteRequest, RequestVoteResponse};
use crate::node::config::RaftConfig;
use crate::node::core::RaftNode;
use crate::state::NodeRole;
use crate::storage::MemStorage;

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ModelRequestVote {
    term: i64,
    candidate_id: i64,
    last_log_index: i64,
    last_log_term: i64,
    group_id: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ModelRequestVoteResponse {
    term: i64,
    vote_granted: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ModelVoteRequestEnvelope {
    src: i64,
    dst: i64,
    req: ModelRequestVote,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ModelVoteResponseEnvelope {
    src: i64,
    dst: i64,
    resp: ModelRequestVoteResponse,
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
    voted_for1: i64,
    voted_for2: i64,
    voted_for3: i64,
    leader_id1: i64,
    leader_id2: i64,
    leader_id3: i64,
    last_log_index1: i64,
    last_log_index2: i64,
    last_log_index3: i64,
    last_log_term1: i64,
    last_log_term2: i64,
    last_log_term3: i64,
    votes_received1: Vec<i64>,
    votes_received2: Vec<i64>,
    votes_received3: Vec<i64>,
    vote_requests: Vec<ModelVoteRequestEnvelope>,
    vote_responses: Vec<ModelVoteResponseEnvelope>,
}

#[derive(Default)]
struct ElectionDriver {
    nodes: HashMap<u64, RaftNode<MemStorage>>,
    vote_requests: Vec<ModelVoteRequestEnvelope>,
    vote_responses: Vec<ModelVoteResponseEnvelope>,
}

impl Driver for ElectionDriver {
    type State = ModelState;

    fn step(&mut self, step: &Step) -> Result {
        switch!(step {
            init => self.init()?,
            TickElectionTimeout(n) => self.tick_election_timeout(n)?,
            HandleRequestVote => self.handle_request_vote()?,
            HandleRequestVoteResponse => self.handle_request_vote_response()?,
            Noop => (),
        })
    }
}

impl State<ElectionDriver> for ModelState {
    fn from_driver(driver: &ElectionDriver) -> Result<Self> {
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
            voted_for1: n1.hard_state.voted_for as i64,
            voted_for2: n2.hard_state.voted_for as i64,
            voted_for3: n3.hard_state.voted_for as i64,
            leader_id1: n1.leader_id as i64,
            leader_id2: n2.leader_id as i64,
            leader_id3: n3.leader_id as i64,
            last_log_index1: n1.log.last_index() as i64,
            last_log_index2: n2.log.last_index() as i64,
            last_log_index3: n3.log.last_index() as i64,
            last_log_term1: n1.log.last_term() as i64,
            last_log_term2: n2.log.last_term() as i64,
            last_log_term3: n3.log.last_term() as i64,
            votes_received1: sorted_votes(n1),
            votes_received2: sorted_votes(n2),
            votes_received3: sorted_votes(n3),
            vote_requests: driver.vote_requests.clone(),
            vote_responses: driver.vote_responses.clone(),
        })
    }
}

impl ElectionDriver {
    fn init(&mut self) -> Result {
        self.nodes.clear();
        self.vote_requests.clear();
        self.vote_responses.clear();

        for node_id in [1, 2, 3] {
            let mut node = RaftNode::new(config(node_id), MemStorage::new());
            node.restore()?;
            self.nodes.insert(node_id, node);
        }
        Ok(())
    }

    fn tick_election_timeout(&mut self, n: i64) -> Result {
        let node_id = n as u64;
        let node = self.node_mut(node_id)?;
        node.election_deadline_override(Instant::now() - Duration::from_millis(1));
        node.tick();
        let ready = node.take_ready();
        self.vote_requests.extend(
            ready
                .vote_requests
                .into_iter()
                .map(|(dst, req)| model_vote_request(node_id, dst, req)),
        );
        Ok(())
    }

    fn handle_request_vote(&mut self) -> Result {
        let msg = self.vote_requests.remove(0);
        let req = rust_vote_request(&msg.req);
        let resp = self.node_mut(msg.dst as u64)?.handle_request_vote(&req);
        self.vote_responses.push(ModelVoteResponseEnvelope {
            src: msg.dst,
            dst: msg.src,
            resp: model_vote_response(resp),
        });
        Ok(())
    }

    fn handle_request_vote_response(&mut self) -> Result {
        let msg = self.vote_responses.remove(0);
        let resp = RequestVoteResponse {
            term: msg.resp.term as u64,
            vote_granted: msg.resp.vote_granted,
        };
        self.node_mut(msg.dst as u64)?
            .handle_request_vote_response(msg.src as u64, &resp);
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

fn sorted_votes(node: &RaftNode<MemStorage>) -> Vec<i64> {
    let mut votes: Vec<_> = node
        .votes_received
        .iter()
        .copied()
        .map(|id| id as i64)
        .collect();
    votes.sort_unstable();
    votes
}

fn model_vote_request(src: u64, dst: u64, req: RequestVoteRequest) -> ModelVoteRequestEnvelope {
    ModelVoteRequestEnvelope {
        src: src as i64,
        dst: dst as i64,
        req: ModelRequestVote {
            term: req.term as i64,
            candidate_id: req.candidate_id as i64,
            last_log_index: req.last_log_index as i64,
            last_log_term: req.last_log_term as i64,
            group_id: req.group_id as i64,
        },
    }
}

fn rust_vote_request(req: &ModelRequestVote) -> RequestVoteRequest {
    RequestVoteRequest {
        term: req.term as u64,
        candidate_id: req.candidate_id as u64,
        last_log_index: req.last_log_index as u64,
        last_log_term: req.last_log_term as u64,
        group_id: req.group_id as u64,
    }
}

fn model_vote_response(resp: RequestVoteResponse) -> ModelRequestVoteResponse {
    ModelRequestVoteResponse {
        term: resp.term as i64,
        vote_granted: resp.vote_granted,
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
    spec = "../quint/raft/SingleGroupElection.qnt",
    max_steps = 20,
    max_samples = 100
)]
fn election_matches_quint() -> impl Driver {
    ElectionDriver::default()
}
