//! Raft single-group state machine.
//!
//! Split across files:
//! - [`config`]: `RaftConfig` (including voter/learner lists).
//! - [`core`]: `RaftNode` struct, constructors, simple accessors, `tick`,
//!   `propose`, and the `Ready` output type.
//! - [`internal`]: Internal state transitions (elections, replication,
//!   commit advancement) and timeout math.
//! - [`membership`]: Dynamic configuration changes — add/remove voters,
//!   add/remove/promote learners.
//! - [`rpc`]: Incoming RPC handlers (`AppendEntries`, `RequestVote`,
//!   `InstallSnapshot`, and their response handlers).

pub mod config;
pub mod core;
mod internal;
pub mod membership;
pub mod rpc;

#[cfg(test)]
mod quint_connect_core;

pub use self::config::RaftConfig;
pub use self::core::{RaftNode, Ready};
