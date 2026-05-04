module RestartPersistence {
  type LogEntry = { term: int, index: int, data: int }

  pure val CANDIDATE_2 = 2

  var phase: int
  var role: str
  var currentTerm: int
  var votedFor: int
  var commitIndex: int
  var lastApplied: int
  var leaderId: int
  var snapshotIndex: int
  var snapshotTerm: int
  var log: List[LogEntry]
  var readyHardState: bool
  var storedCurrentTerm: int
  var storedVotedFor: int
  var storedSnapshotIndex: int
  var storedSnapshotTerm: int
  var storedLog: List[LogEntry]
  var voteGrantedTo3: bool

  action init = all {
    phase' = 0,
    role' = "Follower",
    currentTerm' = 0,
    votedFor' = 0,
    commitIndex' = 0,
    lastApplied' = 0,
    leaderId' = 0,
    snapshotIndex' = 0,
    snapshotTerm' = 0,
    log' = List(),
    readyHardState' = false,
    storedCurrentTerm' = 0,
    storedVotedFor' = 0,
    storedSnapshotIndex' = 0,
    storedSnapshotTerm' = 0,
    storedLog' = List(),
    voteGrantedTo3' = false,
  }

  action GrantVoteTo2 = all {
    phase == 0,
    phase' = 1,
    role' = role,
    currentTerm' = 4,
    votedFor' = CANDIDATE_2,
    commitIndex' = commitIndex,
    lastApplied' = lastApplied,
    leaderId' = leaderId,
    snapshotIndex' = snapshotIndex,
    snapshotTerm' = snapshotTerm,
    log' = log,
    readyHardState' = true,
    storedCurrentTerm' = storedCurrentTerm,
    storedVotedFor' = storedVotedFor,
    storedSnapshotIndex' = storedSnapshotIndex,
    storedSnapshotTerm' = storedSnapshotTerm,
    storedLog' = storedLog,
    voteGrantedTo3' = false,
  }

  action PersistReady = all {
    phase == 1,
    readyHardState,
    phase' = 2,
    role' = role,
    currentTerm' = currentTerm,
    votedFor' = votedFor,
    commitIndex' = commitIndex,
    lastApplied' = lastApplied,
    leaderId' = leaderId,
    snapshotIndex' = snapshotIndex,
    snapshotTerm' = snapshotTerm,
    log' = log,
    readyHardState' = false,
    storedCurrentTerm' = currentTerm,
    storedVotedFor' = votedFor,
    storedSnapshotIndex' = storedSnapshotIndex,
    storedSnapshotTerm' = storedSnapshotTerm,
    storedLog' = storedLog,
    voteGrantedTo3' = false,
  }

  action AppendEntry1 = all {
    phase == 2,
    phase' = 3,
    role' = role,
    currentTerm' = currentTerm,
    votedFor' = votedFor,
    commitIndex' = commitIndex,
    lastApplied' = lastApplied,
    leaderId' = leaderId,
    snapshotIndex' = snapshotIndex,
    snapshotTerm' = snapshotTerm,
    log' = List({ term: 4, index: 1, data: 7 }),
    readyHardState' = readyHardState,
    storedCurrentTerm' = storedCurrentTerm,
    storedVotedFor' = storedVotedFor,
    storedSnapshotIndex' = storedSnapshotIndex,
    storedSnapshotTerm' = storedSnapshotTerm,
    storedLog' = List({ term: 4, index: 1, data: 7 }),
    voteGrantedTo3' = false,
  }

  action AppendEntry2 = all {
    phase == 3,
    phase' = 4,
    role' = role,
    currentTerm' = currentTerm,
    votedFor' = votedFor,
    commitIndex' = commitIndex,
    lastApplied' = lastApplied,
    leaderId' = leaderId,
    snapshotIndex' = snapshotIndex,
    snapshotTerm' = snapshotTerm,
    log' = List(
      { term: 4, index: 1, data: 7 },
      { term: 4, index: 2, data: 8 }
    ),
    readyHardState' = readyHardState,
    storedCurrentTerm' = storedCurrentTerm,
    storedVotedFor' = storedVotedFor,
    storedSnapshotIndex' = storedSnapshotIndex,
    storedSnapshotTerm' = storedSnapshotTerm,
    storedLog' = List(
      { term: 4, index: 1, data: 7 },
      { term: 4, index: 2, data: 8 }
    ),
    voteGrantedTo3' = false,
  }

  action AppendEntry3 = all {
    phase == 4,
    phase' = 5,
    role' = role,
    currentTerm' = currentTerm,
    votedFor' = votedFor,
    commitIndex' = commitIndex,
    lastApplied' = lastApplied,
    leaderId' = leaderId,
    snapshotIndex' = snapshotIndex,
    snapshotTerm' = snapshotTerm,
    log' = List(
      { term: 4, index: 1, data: 7 },
      { term: 4, index: 2, data: 8 },
      { term: 4, index: 3, data: 9 }
    ),
    readyHardState' = readyHardState,
    storedCurrentTerm' = storedCurrentTerm,
    storedVotedFor' = storedVotedFor,
    storedSnapshotIndex' = storedSnapshotIndex,
    storedSnapshotTerm' = storedSnapshotTerm,
    storedLog' = List(
      { term: 4, index: 1, data: 7 },
      { term: 4, index: 2, data: 8 },
      { term: 4, index: 3, data: 9 }
    ),
    voteGrantedTo3' = false,
  }

  action ApplySnapshot = all {
    phase == 5,
    phase' = 6,
    role' = role,
    currentTerm' = currentTerm,
    votedFor' = votedFor,
    commitIndex' = commitIndex,
    lastApplied' = lastApplied,
    leaderId' = leaderId,
    snapshotIndex' = 2,
    snapshotTerm' = 4,
    log' = List({ term: 4, index: 3, data: 9 }),
    readyHardState' = readyHardState,
    storedCurrentTerm' = storedCurrentTerm,
    storedVotedFor' = storedVotedFor,
    storedSnapshotIndex' = 2,
    storedSnapshotTerm' = 4,
    storedLog' = List({ term: 4, index: 3, data: 9 }),
    voteGrantedTo3' = false,
  }

  action RestartRestore = all {
    phase == 6,
    phase' = 7,
    role' = "Follower",
    currentTerm' = storedCurrentTerm,
    votedFor' = storedVotedFor,
    commitIndex' = 0,
    lastApplied' = 0,
    leaderId' = 0,
    snapshotIndex' = storedSnapshotIndex,
    snapshotTerm' = storedSnapshotTerm,
    log' = storedLog,
    readyHardState' = false,
    storedCurrentTerm' = storedCurrentTerm,
    storedVotedFor' = storedVotedFor,
    storedSnapshotIndex' = storedSnapshotIndex,
    storedSnapshotTerm' = storedSnapshotTerm,
    storedLog' = storedLog,
    voteGrantedTo3' = false,
  }

  action RequestVoteFrom3SameTerm = all {
    phase == 7,
    phase' = 8,
    role' = role,
    currentTerm' = currentTerm,
    votedFor' = votedFor,
    commitIndex' = commitIndex,
    lastApplied' = lastApplied,
    leaderId' = leaderId,
    snapshotIndex' = snapshotIndex,
    snapshotTerm' = snapshotTerm,
    log' = log,
    readyHardState' = readyHardState,
    storedCurrentTerm' = storedCurrentTerm,
    storedVotedFor' = storedVotedFor,
    storedSnapshotIndex' = storedSnapshotIndex,
    storedSnapshotTerm' = storedSnapshotTerm,
    storedLog' = storedLog,
    voteGrantedTo3' = false,
  }

  action Noop = all {
    phase' = phase,
    role' = role,
    currentTerm' = currentTerm,
    votedFor' = votedFor,
    commitIndex' = commitIndex,
    lastApplied' = lastApplied,
    leaderId' = leaderId,
    snapshotIndex' = snapshotIndex,
    snapshotTerm' = snapshotTerm,
    log' = log,
    readyHardState' = readyHardState,
    storedCurrentTerm' = storedCurrentTerm,
    storedVotedFor' = storedVotedFor,
    storedSnapshotIndex' = storedSnapshotIndex,
    storedSnapshotTerm' = storedSnapshotTerm,
    storedLog' = storedLog,
    voteGrantedTo3' = voteGrantedTo3,
  }

  action step = any {
    GrantVoteTo2,
    PersistReady,
    AppendEntry1,
    AppendEntry2,
    AppendEntry3,
    ApplySnapshot,
    RestartRestore,
    RequestVoteFrom3SameTerm,
    Noop,
  }

  val restoredHardStateMatchesStorage =
    if (phase >= 7) currentTerm == storedCurrentTerm and votedFor == storedVotedFor else true

  val snapshotAndLogPersisted =
    snapshotIndex == storedSnapshotIndex and snapshotTerm == storedSnapshotTerm and log == storedLog

  val restartedVolatileReset =
    if (phase >= 7) commitIndex == 0 and lastApplied == 0 and leaderId == 0 and role == "Follower" else true

  val secondCandidateNotGranted = voteGrantedTo3 == false
}
