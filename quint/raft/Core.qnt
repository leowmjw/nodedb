module Core {
  type LogEntry = { term: int, index: int, data: int }

  pure val NODE_ID = 1
  pure val NOOP = 0
  pure val COMMANDS = Set(1, 2)
  pure val MAX_LOG_LENGTH = 5

  var role: str
  var currentTerm: int
  var votedFor: int
  var commitIndex: int
  var lastApplied: int
  var leaderId: int
  var log: List[LogEntry]
  var readyHardState: bool
  var readyCommitted: List[LogEntry]

  pure def committedEntries(entries, applied, committed) =
    if (applied < committed) {
      entries.slice(applied, committed)
    } else {
      List()
    }

  action init = all {
    role' = "Follower",
    currentTerm' = 0,
    votedFor' = 0,
    commitIndex' = 0,
    lastApplied' = 0,
    leaderId' = 0,
    log' = List(),
    readyHardState' = false,
    readyCommitted' = List(),
  }

  action TickElectionTimeout = all {
    role == "Follower" or role == "Candidate",
    log.length() < MAX_LOG_LENGTH,
    role' = "Leader",
    currentTerm' = currentTerm + 1,
    votedFor' = NODE_ID,
    commitIndex' = log.length() + 1,
    lastApplied' = lastApplied,
    leaderId' = NODE_ID,
    log' = log.append({ term: currentTerm + 1, index: log.length() + 1, data: NOOP }),
    readyHardState' = true,
    readyCommitted' =
      readyCommitted.concat(
        committedEntries(
          log.append({ term: currentTerm + 1, index: log.length() + 1, data: NOOP }),
          lastApplied,
          log.length() + 1
        )
      ),
  }

  action ClientPropose = {
    nondet data = COMMANDS.oneOf()
    all {
      role == "Leader",
      log.length() < MAX_LOG_LENGTH,
      role' = role,
      currentTerm' = currentTerm,
      votedFor' = votedFor,
      commitIndex' = log.length() + 1,
      lastApplied' = lastApplied,
      leaderId' = leaderId,
      log' = log.append({ term: currentTerm, index: log.length() + 1, data: data }),
      readyHardState' = readyHardState,
      readyCommitted' =
        readyCommitted.concat(
          committedEntries(
            log.append({ term: currentTerm, index: log.length() + 1, data: data }),
            lastApplied,
            log.length() + 1
          )
        ),
    }
  }

  action TakeReady = all {
    readyHardState or readyCommitted.length() > 0,
    role' = role,
    currentTerm' = currentTerm,
    votedFor' = votedFor,
    commitIndex' = commitIndex,
    lastApplied' = lastApplied,
    leaderId' = leaderId,
    log' = log,
    readyHardState' = false,
    readyCommitted' = List(),
  }

  action AdvanceApplied = all {
    lastApplied < commitIndex,
    role' = role,
    currentTerm' = currentTerm,
    votedFor' = votedFor,
    commitIndex' = commitIndex,
    lastApplied' = commitIndex,
    leaderId' = leaderId,
    log' = log,
    readyHardState' = readyHardState,
    readyCommitted' = readyCommitted,
  }

  action Noop = all {
    role' = role,
    currentTerm' = currentTerm,
    votedFor' = votedFor,
    commitIndex' = commitIndex,
    lastApplied' = lastApplied,
    leaderId' = leaderId,
    log' = log,
    readyHardState' = readyHardState,
    readyCommitted' = readyCommitted,
  }

  action step = any {
    TickElectionTimeout,
    ClientPropose,
    TakeReady,
    AdvanceApplied,
    Noop,
  }

  val termMonotonic = currentTerm >= 0
  val commitWithinLog = commitIndex <= log.length()
  val appliedWithinCommit = lastApplied <= commitIndex
}
