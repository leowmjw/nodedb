use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use quint_connect::{Driver, Result, State, Step, quint_run, switch};
use serde::Deserialize;

use crate::multi_raft::MultiRaft;
use crate::raft_loop::handle_rpc::TOPOLOGY_GROUP_ID;
use crate::raft_loop::{CommitApplier, RaftLoop};
use crate::routing::RoutingTable;
use crate::rpc_codec::{JoinRequest, LEADER_REDIRECT_PREFIX, RaftRpc};
use crate::topology::{CLUSTER_WIRE_FORMAT_VERSION, ClusterTopology, NodeInfo, NodeState};
use crate::transport::{NexarTransport, RaftRpcHandler};
use nodedb_raft::AppendEntriesRequest;
use nodedb_raft::message::LogEntry;

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ModelState {
    group_count: i64,
    group0_leader: i64,
    topology_version: i64,
    topology_nodes: i64,
    topology_has2: bool,
    topology_has7: bool,
    learner2_in0: bool,
    learner2_in1: bool,
    learner2_in2: bool,
    member2_in0: bool,
    member2_in1: bool,
    member2_in2: bool,
    last_join_kind: String,
    last_join_mutated: bool,
    last_response_success: bool,
    last_response_error_kind: String,
    last_response_node_count: i64,
    last_response_group_count: i64,
    last_response_learner_all_groups: bool,
}

struct NoopApplier;

impl CommitApplier for NoopApplier {
    fn apply_committed(&self, _group_id: u64, entries: &[LogEntry]) -> u64 {
        entries.last().map(|entry| entry.index).unwrap_or(0)
    }
}

struct JoinDriver {
    runtime: tokio::runtime::Runtime,
    transport: Arc<NexarTransport>,
    raft_loop: RaftLoop<NoopApplier>,
    topology: Arc<RwLock<ClusterTopology>>,
    _tempdir: tempfile::TempDir,
    last_join_kind: String,
    last_join_mutated: bool,
    last_response_success: bool,
    last_response_error_kind: String,
    last_response_node_count: i64,
    last_response_group_count: i64,
    last_response_learner_all_groups: bool,
}

impl Driver for JoinDriver {
    type State = ModelState;

    fn step(&mut self, step: &Step) -> Result {
        switch!(step {
            init => self.init()?,
            SetRemoteLeaderKnown => self.set_remote_leader_known()?,
            HandleJoinRedirect => self.handle_join_redirect()?,
            HandleJoinNew => self.handle_join_new()?,
            HandleJoinIdempotent => self.handle_join_idempotent()?,
            HandleJoinConflict => self.handle_join_conflict()?,
            Noop => (),
        })
    }
}

impl State<JoinDriver> for ModelState {
    fn from_driver(driver: &JoinDriver) -> Result<Self> {
        let topo = driver.topology.read().unwrap_or_else(|p| p.into_inner());
        let routing = {
            let mr = driver
                .raft_loop
                .multi_raft
                .lock()
                .unwrap_or_else(|p| p.into_inner());
            mr.routing().clone()
        };

        let group0 = routing.group_info(0);
        let group1 = routing.group_info(1);
        let group2 = routing.group_info(2);

        Ok(Self {
            group_count: routing.group_ids().len() as i64,
            group0_leader: {
                let mr = driver
                    .raft_loop
                    .multi_raft
                    .lock()
                    .unwrap_or_else(|p| p.into_inner());
                mr.group_leader(TOPOLOGY_GROUP_ID) as i64
            },
            topology_version: topo.version() as i64,
            topology_nodes: topo.node_count() as i64,
            topology_has2: topo.contains(2),
            topology_has7: topo.contains(7),
            learner2_in0: group0.map(|g| g.learners.contains(&2)).unwrap_or(false),
            learner2_in1: group1.map(|g| g.learners.contains(&2)).unwrap_or(false),
            learner2_in2: group2.map(|g| g.learners.contains(&2)).unwrap_or(false),
            member2_in0: group0.map(|g| g.members.contains(&2)).unwrap_or(false),
            member2_in1: group1.map(|g| g.members.contains(&2)).unwrap_or(false),
            member2_in2: group2.map(|g| g.members.contains(&2)).unwrap_or(false),
            last_join_kind: driver.last_join_kind.clone(),
            last_join_mutated: driver.last_join_mutated,
            last_response_success: driver.last_response_success,
            last_response_error_kind: driver.last_response_error_kind.clone(),
            last_response_node_count: driver.last_response_node_count,
            last_response_group_count: driver.last_response_group_count,
            last_response_learner_all_groups: driver.last_response_learner_all_groups,
        })
    }
}

impl JoinDriver {
    fn new() -> Self {
        let runtime = tokio::runtime::Runtime::new().expect("runtime");
        let transport = make_transport(&runtime, 1);
        let (raft_loop, topology, tempdir) = new_bootstrap_loop(transport.clone());
        Self {
            runtime,
            transport,
            raft_loop,
            topology,
            _tempdir: tempdir,
            last_join_kind: "None".to_string(),
            last_join_mutated: false,
            last_response_success: false,
            last_response_error_kind: "None".to_string(),
            last_response_node_count: 0,
            last_response_group_count: 0,
            last_response_learner_all_groups: false,
        }
    }

    fn init(&mut self) -> Result {
        let (raft_loop, topology, tempdir) = new_bootstrap_loop(self.transport.clone());
        self.raft_loop = raft_loop;
        self.topology = topology;
        self._tempdir = tempdir;
        self.reset_response("None", false, false, "None", 0, 0, false);
        Ok(())
    }

    fn set_remote_leader_known(&mut self) -> Result {
        {
            let mut topo = self.topology.write().unwrap_or_else(|p| p.into_inner());
            topo.add_node(NodeInfo::new(
                7,
                "127.0.0.1:9407".parse().expect("addr"),
                NodeState::Active,
            ));
        }

        let term = {
            let mr = self
                .raft_loop
                .multi_raft
                .lock()
                .unwrap_or_else(|p| p.into_inner());
            mr.group_statuses()
                .into_iter()
                .find(|status| status.group_id == TOPOLOGY_GROUP_ID)
                .map(|status| status.term)
                .unwrap_or(0)
        };
        let req = AppendEntriesRequest {
            term: term + 1,
            leader_id: 7,
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![],
            leader_commit: 0,
            group_id: TOPOLOGY_GROUP_ID,
        };
        {
            let mut mr = self
                .raft_loop
                .multi_raft
                .lock()
                .unwrap_or_else(|p| p.into_inner());
            let _ = mr.handle_append_entries(&req)?;
        }
        self.reset_response("None", false, false, "None", 0, 0, false);
        Ok(())
    }

    fn handle_join_redirect(&mut self) -> Result {
        let before = self
            .topology
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .version();
        let resp = run_join(
            &self.runtime,
            &self.raft_loop,
            JoinRequest {
                node_id: 2,
                listen_addr: "127.0.0.1:9401".into(),
                wire_version: CLUSTER_WIRE_FORMAT_VERSION,
                spiffe_id: None,
                spki_pin: None,
            },
        )?;
        let after = self
            .topology
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .version();
        self.reset_response(
            "Redirect",
            before != after,
            resp.success,
            if resp.error.starts_with(LEADER_REDIRECT_PREFIX) {
                "Redirect"
            } else {
                "Other"
            },
            resp.nodes.len() as i64,
            resp.groups.len() as i64,
            false,
        );
        Ok(())
    }

    fn handle_join_new(&mut self) -> Result {
        let before = self
            .topology
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .version();
        let resp = run_join(
            &self.runtime,
            &self.raft_loop,
            JoinRequest {
                node_id: 2,
                listen_addr: "127.0.0.1:9401".into(),
                wire_version: CLUSTER_WIRE_FORMAT_VERSION,
                spiffe_id: None,
                spki_pin: None,
            },
        )?;
        let after = self
            .topology
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .version();
        let learner_everywhere = resp.groups.iter().all(|group| group.learners.contains(&2));
        self.reset_response(
            "New",
            before != after,
            resp.success,
            if resp.error.is_empty() {
                "None"
            } else {
                "Other"
            },
            resp.nodes.len() as i64,
            resp.groups.len() as i64,
            learner_everywhere,
        );
        Ok(())
    }

    fn handle_join_idempotent(&mut self) -> Result {
        let before = self
            .topology
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .version();
        let resp = run_join(
            &self.runtime,
            &self.raft_loop,
            JoinRequest {
                node_id: 2,
                listen_addr: "127.0.0.1:9401".into(),
                wire_version: CLUSTER_WIRE_FORMAT_VERSION,
                spiffe_id: None,
                spki_pin: None,
            },
        )?;
        let after = self
            .topology
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .version();
        let learner_everywhere = resp.groups.iter().all(|group| group.learners.contains(&2));
        self.reset_response(
            "Idempotent",
            before != after,
            resp.success,
            if resp.error.is_empty() {
                "None"
            } else {
                "Other"
            },
            resp.nodes.len() as i64,
            resp.groups.len() as i64,
            learner_everywhere,
        );
        Ok(())
    }

    fn handle_join_conflict(&mut self) -> Result {
        let before = self
            .topology
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .version();
        let resp = run_join(
            &self.runtime,
            &self.raft_loop,
            JoinRequest {
                node_id: 2,
                listen_addr: "127.0.0.1:9502".into(),
                wire_version: CLUSTER_WIRE_FORMAT_VERSION,
                spiffe_id: None,
                spki_pin: None,
            },
        )?;
        let after = self
            .topology
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .version();
        self.reset_response(
            "Conflict",
            before != after,
            resp.success,
            if !resp.success
                && !resp.error.starts_with(LEADER_REDIRECT_PREFIX)
                && !resp.error.is_empty()
            {
                "Conflict"
            } else {
                "Other"
            },
            resp.nodes.len() as i64,
            resp.groups.len() as i64,
            false,
        );
        Ok(())
    }

    fn reset_response(
        &mut self,
        join_kind: &str,
        mutated: bool,
        success: bool,
        error_kind: &str,
        node_count: i64,
        group_count: i64,
        learner_all_groups: bool,
    ) {
        self.last_join_kind = join_kind.to_string();
        self.last_join_mutated = mutated;
        self.last_response_success = success;
        self.last_response_error_kind = error_kind.to_string();
        self.last_response_node_count = node_count;
        self.last_response_group_count = group_count;
        self.last_response_learner_all_groups = learner_all_groups;
    }
}

fn new_bootstrap_loop(
    transport: Arc<NexarTransport>,
) -> (
    RaftLoop<NoopApplier>,
    Arc<RwLock<ClusterTopology>>,
    tempfile::TempDir,
) {
    let tempdir = tempfile::tempdir().expect("tempdir");
    let rt = RoutingTable::uniform(2, &[1], 1);
    let mut mr = MultiRaft::new(1, rt, tempdir.path().to_path_buf());
    mr.add_group(0, vec![]).expect("group0");
    mr.add_group(1, vec![]).expect("group1");
    mr.add_group(2, vec![]).expect("group2");
    for node in mr.groups_mut().values_mut() {
        node.election_deadline_override(Instant::now() - Duration::from_millis(1));
    }

    let mut topology = ClusterTopology::new();
    topology.add_node(NodeInfo::new(
        1,
        "127.0.0.1:9400".parse().expect("addr"),
        NodeState::Active,
    ));
    let topology = Arc::new(RwLock::new(topology));
    let raft_loop = RaftLoop::new(mr, transport, topology.clone(), NoopApplier);
    raft_loop.do_tick();
    (raft_loop, topology, tempdir)
}

fn make_transport(runtime: &tokio::runtime::Runtime, node_id: u64) -> Arc<NexarTransport> {
    let _guard = runtime.enter();
    Arc::new(
        NexarTransport::new(
            node_id,
            "127.0.0.1:0".parse().expect("addr"),
            crate::transport::credentials::TransportCredentials::Insecure,
        )
        .expect("transport"),
    )
}

fn run_join(
    runtime: &tokio::runtime::Runtime,
    raft_loop: &RaftLoop<NoopApplier>,
    req: JoinRequest,
) -> Result<crate::rpc_codec::JoinResponse> {
    match runtime.block_on(raft_loop.handle_rpc(RaftRpc::JoinRequest(req)))? {
        RaftRpc::JoinResponse(resp) => Ok(resp),
        other => Err(std::io::Error::other(format!("expected JoinResponse, got {other:?}")).into()),
    }
}

#[quint_run(spec = "../quint/raft/Join.qnt", max_steps = 10, max_samples = 40)]
fn join_matches_quint() -> impl Driver {
    JoinDriver::new()
}
