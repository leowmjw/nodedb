use std::sync::RwLock;

use quint_connect::{Driver, Result, State, Step, quint_run, switch};
use serde::Deserialize;

use crate::control::distributed_applier::ProposeTracker;
use crate::control::gateway::retry::retry_not_leader;
use crate::types::VShardId;
use nodedb_cluster::RoutingTable;

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ModelState {
    last_case: String,
    receiver_resolved: bool,
    last_result_kind: String,
    last_payload_len: i64,
    last_expected_key: i64,
    last_applied_key: i64,
    last_stored_before_register: bool,
    last_retry_attempts: i64,
    last_routing_leader: i64,
}

struct ApplyAckIdentityDriver {
    runtime: tokio::runtime::Runtime,
    last_case: String,
    receiver_resolved: bool,
    last_result_kind: String,
    last_payload_len: i64,
    last_expected_key: i64,
    last_applied_key: i64,
    last_stored_before_register: bool,
    last_retry_attempts: i64,
    last_routing_leader: i64,
}

impl Driver for ApplyAckIdentityDriver {
    type State = ModelState;

    fn step(&mut self, step: &Step) -> Result {
        switch!(step {
            init => self.init(),
            RegisterThenCompleteMatch => self.register_then_complete_match(),
            RegisterThenCompleteMismatch => self.register_then_complete_mismatch(),
            CompleteThenRegisterMatch => self.complete_then_register_match(),
            CompleteThenRegisterMismatch => self.complete_then_register_mismatch(),
            CompleteNoopOverwriteThenRegister => self.complete_noop_overwrite_then_register(),
            RetryLeaderChangeThenSuccess => self.retry_leader_change_then_success()?,
            RetryNotLeaderThenSuccess => self.retry_not_leader_then_success()?,
            Noop => (),
        });
        Ok(())
    }
}

impl State<ApplyAckIdentityDriver> for ModelState {
    fn from_driver(driver: &ApplyAckIdentityDriver) -> Result<Self> {
        Ok(Self {
            last_case: driver.last_case.clone(),
            receiver_resolved: driver.receiver_resolved,
            last_result_kind: driver.last_result_kind.clone(),
            last_payload_len: driver.last_payload_len,
            last_expected_key: driver.last_expected_key,
            last_applied_key: driver.last_applied_key,
            last_stored_before_register: driver.last_stored_before_register,
            last_retry_attempts: driver.last_retry_attempts,
            last_routing_leader: driver.last_routing_leader,
        })
    }
}

impl ApplyAckIdentityDriver {
    fn new() -> Self {
        Self {
            runtime: tokio::runtime::Runtime::new().expect("runtime"),
            last_case: "None".to_string(),
            receiver_resolved: false,
            last_result_kind: "None".to_string(),
            last_payload_len: 0,
            last_expected_key: 0,
            last_applied_key: 0,
            last_stored_before_register: false,
            last_retry_attempts: 0,
            last_routing_leader: 0,
        }
    }

    fn init(&mut self) {
        self.last_case = "None".to_string();
        self.receiver_resolved = false;
        self.last_result_kind = "None".to_string();
        self.last_payload_len = 0;
        self.last_expected_key = 0;
        self.last_applied_key = 0;
        self.last_stored_before_register = false;
        self.last_retry_attempts = 0;
        self.last_routing_leader = 0;
    }

    fn register_then_complete_match(&mut self) {
        let tracker = ProposeTracker::new();
        let expected_key = 0xaaaa_u64;
        let applied_key = expected_key;
        let mut rx = tracker.register(1, 5, expected_key);
        assert!(tracker.complete(1, 5, applied_key, Ok(b"result".to_vec())));
        let result = rx.try_recv().expect("resolved");
        self.record_result(
            "RegisterThenCompleteMatch",
            true,
            expected_key,
            applied_key,
            false,
            result,
        );
    }

    fn register_then_complete_mismatch(&mut self) {
        let tracker = ProposeTracker::new();
        let expected_key = 0xaaaa_u64;
        let applied_key = 0xbbbb_u64;
        let mut rx = tracker.register(1, 5, expected_key);
        assert!(tracker.complete(1, 5, applied_key, Ok(b"other".to_vec())));
        let result = rx.try_recv().expect("resolved");
        self.record_result(
            "RegisterThenCompleteMismatch",
            true,
            expected_key,
            applied_key,
            false,
            result,
        );
    }

    fn complete_then_register_match(&mut self) {
        let tracker = ProposeTracker::new();
        let expected_key = 0xaaaa_u64;
        let applied_key = expected_key;
        assert!(!tracker.complete(1, 5, applied_key, Ok(b"result".to_vec())));
        let mut rx = tracker.register(1, 5, expected_key);
        let result = rx.try_recv().expect("resolved");
        self.record_result(
            "CompleteThenRegisterMatch",
            true,
            expected_key,
            applied_key,
            true,
            result,
        );
    }

    fn complete_then_register_mismatch(&mut self) {
        let tracker = ProposeTracker::new();
        let expected_key = 0xaaaa_u64;
        let applied_key = 0xbbbb_u64;
        assert!(!tracker.complete(1, 5, applied_key, Ok(b"other".to_vec())));
        let mut rx = tracker.register(1, 5, expected_key);
        let result = rx.try_recv().expect("resolved");
        self.record_result(
            "CompleteThenRegisterMismatch",
            true,
            expected_key,
            applied_key,
            true,
            result,
        );
    }

    fn complete_noop_overwrite_then_register(&mut self) {
        let tracker = ProposeTracker::new();
        let expected_key = 0xaaaa_u64;
        assert!(!tracker.complete(
            1,
            5,
            0,
            Err(crate::Error::RetryableLeaderChange {
                group_id: 1,
                log_index: 5,
            }),
        ));
        let mut rx = tracker.register(1, 5, expected_key);
        let result = rx.try_recv().expect("resolved");
        self.record_result(
            "CompleteNoopOverwriteThenRegister",
            true,
            expected_key,
            0,
            true,
            result,
        );
    }

    fn retry_leader_change_then_success(&mut self) -> Result {
        let mut attempts = 0_i64;
        let result = self.runtime.block_on(retry_not_leader(None, |attempt| {
            attempts = attempt as i64 + 1;
            async move {
                if attempt == 0 {
                    Err(crate::Error::RetryableLeaderChange {
                        group_id: 1,
                        log_index: 5,
                    })
                } else {
                    Ok::<Vec<u8>, crate::Error>(b"settled".to_vec())
                }
            }
        }))?;
        self.last_case = "RetryLeaderChangeThenSuccess".to_string();
        self.receiver_resolved = true;
        self.last_result_kind = "Ok".to_string();
        self.last_payload_len = result.len() as i64;
        self.last_expected_key = 0;
        self.last_applied_key = 0;
        self.last_stored_before_register = false;
        self.last_retry_attempts = attempts;
        self.last_routing_leader = 0;
        Ok(())
    }

    fn retry_not_leader_then_success(&mut self) -> Result {
        let routing = RwLock::new(RoutingTable::uniform(1, &[1, 2], 2));
        let mut attempts = 0_i64;
        let result = self.runtime.block_on(retry_not_leader(Some(&routing), |attempt| {
            attempts = attempt as i64 + 1;
            async move {
                if attempt == 0 {
                    Err(crate::Error::NotLeader {
                        vshard_id: VShardId::new(0),
                        leader_node: 9,
                        leader_addr: "10.0.0.9:9400".into(),
                    })
                } else {
                    Ok::<Vec<u8>, crate::Error>(b"settled".to_vec())
                }
            }
        }))?;
        self.last_case = "RetryNotLeaderThenSuccess".to_string();
        self.receiver_resolved = true;
        self.last_result_kind = "Ok".to_string();
        self.last_payload_len = result.len() as i64;
        self.last_expected_key = 0;
        self.last_applied_key = 0;
        self.last_stored_before_register = false;
        self.last_retry_attempts = attempts;
        self.last_routing_leader = routing
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .leader_for_vshard(0)
            .unwrap_or(0) as i64;
        Ok(())
    }

    fn record_result(
        &mut self,
        case: &str,
        resolved: bool,
        expected_key: u64,
        applied_key: u64,
        stored_before_register: bool,
        result: crate::control::wal_replication::ProposeResult,
    ) {
        self.last_case = case.to_string();
        self.receiver_resolved = resolved;
        self.last_expected_key = expected_key as i64;
        self.last_applied_key = applied_key as i64;
        self.last_stored_before_register = stored_before_register;
        self.last_retry_attempts = 0;
        self.last_routing_leader = 0;
        match result {
            Ok(payload) => {
                self.last_result_kind = "Ok".to_string();
                self.last_payload_len = payload.len() as i64;
            }
            Err(crate::Error::RetryableLeaderChange { .. }) => {
                self.last_result_kind = "RetryableLeaderChange".to_string();
                self.last_payload_len = 0;
            }
            Err(other) => panic!("unexpected result kind: {other}"),
        }
    }
}

#[quint_run(
    spec = "../../../../quint/raft/ApplyAckIdentity.qnt",
    max_steps = 10,
    max_samples = 80
)]
fn apply_ack_identity_matches_quint() -> impl Driver {
    ApplyAckIdentityDriver::new()
}
