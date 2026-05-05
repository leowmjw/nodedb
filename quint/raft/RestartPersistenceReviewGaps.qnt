module RestartPersistenceReviewGaps {
  var phase: int
  var currentTerm: int
  var votedFor: int
  var storedCurrentTerm: int
  var storedVotedFor: int
  var readyHardState: bool
  var protocolVisible: bool
  var snapshotIndex: int
  var snapshotTerm: int
  var storedSnapshotIndex: int
  var storedSnapshotTerm: int
  var commitIndex: int
  var lastApplied: int

  action init = all {
    phase' = 0,
    currentTerm' = 0,
    votedFor' = 0,
    storedCurrentTerm' = 0,
    storedVotedFor' = 0,
    readyHardState' = false,
    protocolVisible' = false,
    snapshotIndex' = 0,
    snapshotTerm' = 0,
    storedSnapshotIndex' = 0,
    storedSnapshotTerm' = 0,
    commitIndex' = 0,
    lastApplied' = 0,
  }

  action GrantVote = all {
    phase == 0,
    phase' = 1,
    currentTerm' = 4,
    votedFor' = 2,
    storedCurrentTerm' = storedCurrentTerm,
    storedVotedFor' = storedVotedFor,
    readyHardState' = true,
    protocolVisible' = false,
    snapshotIndex' = snapshotIndex,
    snapshotTerm' = snapshotTerm,
    storedSnapshotIndex' = storedSnapshotIndex,
    storedSnapshotTerm' = storedSnapshotTerm,
    commitIndex' = commitIndex,
    lastApplied' = lastApplied,
  }

  action PersistFailsButReadyEscapes = all {
    phase == 1,
    readyHardState,
    phase' = 2,
    currentTerm' = currentTerm,
    votedFor' = votedFor,
    storedCurrentTerm' = storedCurrentTerm,
    storedVotedFor' = storedVotedFor,
    readyHardState' = readyHardState,
    protocolVisible' = true,
    snapshotIndex' = snapshotIndex,
    snapshotTerm' = snapshotTerm,
    storedSnapshotIndex' = storedSnapshotIndex,
    storedSnapshotTerm' = storedSnapshotTerm,
    commitIndex' = commitIndex,
    lastApplied' = lastApplied,
  }

  action ApplySnapshot = all {
    phase == 2,
    phase' = 3,
    currentTerm' = currentTerm,
    votedFor' = votedFor,
    storedCurrentTerm' = storedCurrentTerm,
    storedVotedFor' = storedVotedFor,
    readyHardState' = readyHardState,
    protocolVisible' = protocolVisible,
    snapshotIndex' = 2,
    snapshotTerm' = 4,
    storedSnapshotIndex' = 2,
    storedSnapshotTerm' = 4,
    commitIndex' = 2,
    lastApplied' = 2,
  }

  action RestartWithoutAppliedWatermark = all {
    phase == 3,
    phase' = 4,
    currentTerm' = storedCurrentTerm,
    votedFor' = storedVotedFor,
    storedCurrentTerm' = storedCurrentTerm,
    storedVotedFor' = storedVotedFor,
    readyHardState' = false,
    protocolVisible' = false,
    snapshotIndex' = storedSnapshotIndex,
    snapshotTerm' = storedSnapshotTerm,
    storedSnapshotIndex' = storedSnapshotIndex,
    storedSnapshotTerm' = storedSnapshotTerm,
    commitIndex' = 0,
    lastApplied' = 0,
  }

  action Noop = all {
    phase' = phase,
    currentTerm' = currentTerm,
    votedFor' = votedFor,
    storedCurrentTerm' = storedCurrentTerm,
    storedVotedFor' = storedVotedFor,
    readyHardState' = readyHardState,
    protocolVisible' = protocolVisible,
    snapshotIndex' = snapshotIndex,
    snapshotTerm' = snapshotTerm,
    storedSnapshotIndex' = storedSnapshotIndex,
    storedSnapshotTerm' = storedSnapshotTerm,
    commitIndex' = commitIndex,
    lastApplied' = lastApplied,
  }

  action step = any {
    GrantVote,
    PersistFailsButReadyEscapes,
    ApplySnapshot,
    RestartWithoutAppliedWatermark,
    Noop,
  }

  val durableBeforeProtocol =
    if (protocolVisible)
      storedCurrentTerm == currentTerm and storedVotedFor == votedFor
    else true

  val restoredWatermarkMatchesSnapshot =
    if (phase >= 4)
      commitIndex == snapshotIndex and lastApplied == snapshotIndex
    else true
}
