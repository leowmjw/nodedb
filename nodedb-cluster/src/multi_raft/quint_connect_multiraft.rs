use std::collections::HashMap;
use std::time::{Duration, Instant};

use quint_connect::{Driver, Result, State, Step, quint_run, switch};
use serde::Deserialize;

use crate::error::ClusterError;
use crate::multi_raft::MultiRaft;
use crate::routing::{GroupInfo, RoutingTable};

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ModelState {
    mounted0: bool,
    mounted1: bool,
    mounted2: bool,
    group_count: i64,
    route_shard0: i64,
    route_shard1: i64,
    vshards0: i64,
    vshards1: i64,
    vshards2: i64,
    role0: String,
    role1: String,
    role2: String,
    leader0: i64,
    leader1: i64,
    leader2: i64,
    term0: i64,
    term1: i64,
    term2: i64,
    commit0: i64,
    commit1: i64,
    commit2: i64,
    ready_commit0: i64,
    ready_commit1: i64,
    ready_commit2: i64,
    applied0: i64,
    applied1: i64,
    applied2: i64,
    member_count0: i64,
    member_count1: i64,
    member_count2: i64,
    last_proposal_kind: String,
    last_proposal_group: i64,
    last_proposal_ok: bool,
    last_proposal_not_leader: bool,
    last_proposal_index: i64,
    last_ready_groups: Vec<i64>,
    last_ready_committed_total: i64,
}

struct MultiRaftDriver {
    mr: MultiRaft,
    tempdir: tempfile::TempDir,
    last_ready: Vec<(u64, u64)>,
    ready_commit: HashMap<u64, u64>,
    last_ready_groups: Vec<i64>,
    last_ready_committed_total: i64,
    last_proposal_kind: String,
    last_proposal_group: i64,
    last_proposal_ok: bool,
    last_proposal_not_leader: bool,
    last_proposal_index: i64,
}

impl Driver for MultiRaftDriver {
    type State = ModelState;

    fn step(&mut self, step: &Step) -> Result {
        switch!(step {
            init => self.init()?,
            MountVotingGroups => self.mount_voting_groups()?,
            MountLearnerGroup => self.mount_learner_group()?,
            TickAll => self.tick_all()?,
            AdvanceReady => self.advance_ready()?,
            ProposeVShard0 => self.propose_vshard0()?,
            ProposeVShard1 => self.propose_vshard1()?,
            ProposeMetadata => self.propose_metadata()?,
            Noop => (),
        })
    }
}

impl State<MultiRaftDriver> for ModelState {
    fn from_driver(driver: &MultiRaftDriver) -> Result<Self> {
        let statuses = driver.mr.group_statuses();
        let routing = driver.mr.routing();

        Ok(Self {
            mounted0: driver.mr.groups.contains_key(&0),
            mounted1: driver.mr.groups.contains_key(&1),
            mounted2: driver.mr.groups.contains_key(&2),
            group_count: driver.mr.group_count() as i64,
            route_shard0: routing.group_for_vshard(0)? as i64,
            route_shard1: routing.group_for_vshard(1)? as i64,
            vshards0: routing.vshards_for_group(0).len() as i64,
            vshards1: routing.vshards_for_group(1).len() as i64,
            vshards2: routing.vshards_for_group(2).len() as i64,
            role0: status_role(&statuses, 0),
            role1: status_role(&statuses, 1),
            role2: status_role(&statuses, 2),
            leader0: status_leader(&statuses, 0),
            leader1: status_leader(&statuses, 1),
            leader2: status_leader(&statuses, 2),
            term0: status_term(&statuses, 0),
            term1: status_term(&statuses, 1),
            term2: status_term(&statuses, 2),
            commit0: status_commit(&statuses, 0),
            commit1: status_commit(&statuses, 1),
            commit2: status_commit(&statuses, 2),
            ready_commit0: *driver.ready_commit.get(&0).unwrap_or(&0) as i64,
            ready_commit1: *driver.ready_commit.get(&1).unwrap_or(&0) as i64,
            ready_commit2: *driver.ready_commit.get(&2).unwrap_or(&0) as i64,
            applied0: status_applied(&statuses, 0),
            applied1: status_applied(&statuses, 1),
            applied2: status_applied(&statuses, 2),
            member_count0: status_members(&statuses, 0),
            member_count1: status_members(&statuses, 1),
            member_count2: status_members(&statuses, 2),
            last_proposal_kind: driver.last_proposal_kind.clone(),
            last_proposal_group: driver.last_proposal_group,
            last_proposal_ok: driver.last_proposal_ok,
            last_proposal_not_leader: driver.last_proposal_not_leader,
            last_proposal_index: driver.last_proposal_index,
            last_ready_groups: driver.last_ready_groups.clone(),
            last_ready_committed_total: driver.last_ready_committed_total,
        })
    }
}

impl MultiRaftDriver {
    fn new() -> Self {
        let tempdir = tempfile::tempdir().expect("tempdir");
        Self {
            mr: MultiRaft::new(1, build_routing(), tempdir.path().to_path_buf()),
            tempdir,
            last_ready: Vec::new(),
            ready_commit: HashMap::new(),
            last_ready_groups: Vec::new(),
            last_ready_committed_total: 0,
            last_proposal_kind: "None".to_string(),
            last_proposal_group: -1,
            last_proposal_ok: false,
            last_proposal_not_leader: false,
            last_proposal_index: 0,
        }
    }

    fn init(&mut self) -> Result {
        let tempdir = tempfile::tempdir()?;
        self.mr = MultiRaft::new(1, build_routing(), tempdir.path().to_path_buf());
        self.tempdir = tempdir;
        self.last_ready.clear();
        self.ready_commit.clear();
        self.last_ready_groups.clear();
        self.last_ready_committed_total = 0;
        self.reset_proposal();
        Ok(())
    }

    fn mount_voting_groups(&mut self) -> Result {
        self.clear_ready();
        self.reset_proposal();
        self.mr.add_group(0, vec![])?;
        self.mr.add_group(1, vec![])?;
        Ok(())
    }

    fn mount_learner_group(&mut self) -> Result {
        self.clear_ready();
        self.reset_proposal();
        self.mr.add_group_as_learner(2, vec![9], vec![])?;
        Ok(())
    }

    fn tick_all(&mut self) -> Result {
        self.reset_proposal();
        for gid in [0_u64, 1_u64] {
            if let Some(node) = self.mr.groups_mut().get_mut(&gid)
                && node.current_term() == 0
            {
                node.election_deadline_override(Instant::now() - Duration::from_millis(1));
            }
        }
        let ready = self.mr.tick()?;
        let mut groups = Vec::new();
        let mut pending = Vec::new();
        let mut committed_total = 0;
        for (gid, group_ready) in ready.groups {
            groups.push(gid as i64);
            committed_total += group_ready.committed_entries.len() as i64;
            if let Some(last) = group_ready.committed_entries.last() {
                pending.push((gid, last.index));
                self.ready_commit.insert(gid, last.index);
            }
        }
        groups.sort_unstable();
        self.last_ready = pending;
        self.last_ready_groups = groups;
        self.last_ready_committed_total = committed_total;
        Ok(())
    }

    fn advance_ready(&mut self) -> Result {
        self.reset_proposal();
        let pending = std::mem::take(&mut self.last_ready);
        for (gid, index) in pending {
            self.mr.advance_applied(gid, index)?;
        }
        self.last_ready_groups.clear();
        self.last_ready_committed_total = 0;
        Ok(())
    }

    fn propose_vshard0(&mut self) -> Result {
        self.clear_ready();
        self.last_proposal_kind = "VShard0".to_string();
        let (gid, index) = self.mr.propose(0, b"cmd-vshard-0".to_vec())?;
        self.last_proposal_group = gid as i64;
        self.last_proposal_ok = true;
        self.last_proposal_not_leader = false;
        self.last_proposal_index = index as i64;
        Ok(())
    }

    fn propose_vshard1(&mut self) -> Result {
        self.clear_ready();
        self.last_proposal_kind = "VShard1".to_string();
        match self.mr.propose(1, b"cmd-vshard-1".to_vec()) {
            Ok((gid, index)) => {
                self.last_proposal_group = gid as i64;
                self.last_proposal_ok = true;
                self.last_proposal_not_leader = false;
                self.last_proposal_index = index as i64;
                Ok(())
            }
            Err(ClusterError::Raft(nodedb_raft::RaftError::NotLeader { .. })) => {
                self.last_proposal_group = 2;
                self.last_proposal_ok = false;
                self.last_proposal_not_leader = true;
                self.last_proposal_index = 0;
                Ok(())
            }
            Err(err) => Err(err.into()),
        }
    }

    fn propose_metadata(&mut self) -> Result {
        self.clear_ready();
        self.last_proposal_kind = "Metadata".to_string();
        let index = self.mr.propose_to_group(0, b"cmd-meta".to_vec())?;
        self.last_proposal_group = 0;
        self.last_proposal_ok = true;
        self.last_proposal_not_leader = false;
        self.last_proposal_index = index as i64;
        Ok(())
    }

    fn clear_ready(&mut self) {
        self.last_ready.clear();
        self.last_ready_groups.clear();
        self.last_ready_committed_total = 0;
    }

    fn reset_proposal(&mut self) {
        self.last_proposal_kind = "None".to_string();
        self.last_proposal_group = -1;
        self.last_proposal_ok = false;
        self.last_proposal_not_leader = false;
        self.last_proposal_index = 0;
    }
}

fn build_routing() -> RoutingTable {
    let mut vshard_to_group = Vec::with_capacity(crate::routing::VSHARD_COUNT as usize);
    for i in 0..crate::routing::VSHARD_COUNT {
        vshard_to_group.push(if i % 2 == 0 { 1 } else { 2 });
    }

    let mut group_members = HashMap::new();
    group_members.insert(
        0,
        GroupInfo {
            leader: 1,
            members: vec![1],
            learners: Vec::new(),
        },
    );
    group_members.insert(
        1,
        GroupInfo {
            leader: 1,
            members: vec![1],
            learners: Vec::new(),
        },
    );
    group_members.insert(
        2,
        GroupInfo {
            leader: 0,
            members: vec![9],
            learners: vec![1],
        },
    );
    RoutingTable::from_parts(vshard_to_group, group_members)
}

fn status_role(statuses: &[crate::multi_raft::GroupStatus], gid: u64) -> String {
    statuses
        .iter()
        .find(|status| status.group_id == gid)
        .map(|status| status.role.clone())
        .unwrap_or_else(|| "Absent".to_string())
}

fn status_leader(statuses: &[crate::multi_raft::GroupStatus], gid: u64) -> i64 {
    statuses
        .iter()
        .find(|status| status.group_id == gid)
        .map(|status| status.leader_id as i64)
        .unwrap_or(0)
}

fn status_term(statuses: &[crate::multi_raft::GroupStatus], gid: u64) -> i64 {
    statuses
        .iter()
        .find(|status| status.group_id == gid)
        .map(|status| status.term as i64)
        .unwrap_or(0)
}

fn status_commit(statuses: &[crate::multi_raft::GroupStatus], gid: u64) -> i64 {
    statuses
        .iter()
        .find(|status| status.group_id == gid)
        .map(|status| status.commit_index as i64)
        .unwrap_or(0)
}

fn status_applied(statuses: &[crate::multi_raft::GroupStatus], gid: u64) -> i64 {
    statuses
        .iter()
        .find(|status| status.group_id == gid)
        .map(|status| status.last_applied as i64)
        .unwrap_or(0)
}

fn status_members(statuses: &[crate::multi_raft::GroupStatus], gid: u64) -> i64 {
    statuses
        .iter()
        .find(|status| status.group_id == gid)
        .map(|status| status.member_count as i64)
        .unwrap_or(0)
}

#[quint_run(
    spec = "../quint/raft/MultiRaft.qnt",
    max_steps = 12,
    max_samples = 100
)]
fn multiraft_matches_quint() -> impl Driver {
    MultiRaftDriver::new()
}
