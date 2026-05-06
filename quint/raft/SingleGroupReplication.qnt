module SingleGroupReplication {
  type LogEntry = { term: int, index: int, data: int }
  type AppendEntries = {
    term: int,
    leaderId: int,
    prevLogIndex: int,
    prevLogTerm: int,
    entries: List[LogEntry],
    leaderCommit: int,
    groupId: int,
  }
  type AppendEntriesResponse = {
    term: int,
    success: bool,
    lastLogIndex: int,
  }
  type AppendEnvelope = { src: int, dst: int, req: AppendEntries }
  type AppendResponseEnvelope = { src: int, dst: int, resp: AppendEntriesResponse }

  pure val GROUP_ID = 1
  pure val LEADER = 1
  pure val NOOP = 0
  pure val COMMANDS = Set(3, 4)
  pure val MAX_LOG_LENGTH = 3

  var role1: str
  var role2: str
  var role3: str
  var currentTerm1: int
  var currentTerm2: int
  var currentTerm3: int
  var leaderId1: int
  var leaderId2: int
  var leaderId3: int
  var commitIndex1: int
  var commitIndex2: int
  var commitIndex3: int
  var lastApplied1: int
  var lastApplied2: int
  var lastApplied3: int
  var readyCommitIndex1: int
  var readyCommitIndex2: int
  var readyCommitIndex3: int
  var log1: List[LogEntry]
  var log2: List[LogEntry]
  var log3: List[LogEntry]
  var nextIndex2: int
  var nextIndex3: int
  var matchIndex2: int
  var matchIndex3: int
  var appendMessages: List[AppendEnvelope]
  var appendResponses: List[AppendResponseEnvelope]
  var readyCommitted1: List[LogEntry]
  var readyCommitted2: List[LogEntry]
  var readyCommitted3: List[LogEntry]

  pure def noopEntry = { term: 1, index: 1, data: NOOP }
  pure def leaderEntry2 = { term: 3, index: 2, data: 1 }
  pure def conflictEntry2 = { term: 2, index: 2, data: 2 }

  def roleOf(n) =
    if (n == 1) role1 else if (n == 2) role2 else role3

  def termOf(n) =
    if (n == 1) currentTerm1 else if (n == 2) currentTerm2 else currentTerm3

  def leaderIdOf(n) =
    if (n == 1) leaderId1 else if (n == 2) leaderId2 else leaderId3

  def commitIndexOf(n) =
    if (n == 1) commitIndex1 else if (n == 2) commitIndex2 else commitIndex3

  def lastAppliedOf(n) =
    if (n == 1) lastApplied1 else if (n == 2) lastApplied2 else lastApplied3

  def readyCommitIndexOf(n) =
    if (n == 1) readyCommitIndex1 else if (n == 2) readyCommitIndex2 else readyCommitIndex3

  def logOf(n) =
    if (n == 1) log1 else if (n == 2) log2 else log3

  def readyCommittedOf(n) =
    if (n == 1) readyCommitted1 else if (n == 2) readyCommitted2 else readyCommitted3

  def nextIndexOf(peer) =
    if (peer == 2) nextIndex2 else nextIndex3

  def matchIndexOf(peer) =
    if (peer == 2) matchIndex2 else matchIndex3

  pure def maxInt(a, b) =
    if (a >= b) a else b

  pure def termAt(entries, index) =
    if (index == 0) {
      0
    } else if (index <= entries.length()) {
      entries[index - 1].term
    } else {
      -1
    }

  pure def entriesFrom(entries, fromIndex) =
    if (fromIndex <= entries.length()) {
      entries.slice(fromIndex - 1, entries.length())
    } else {
      List()
    }

  pure def appendEntriesToLog(entries, prevLogIndex, newEntries) =
    entries.slice(0, prevLogIndex).concat(newEntries)

  pure def committedEntries(entries, applied, emitted, committed) =
    if (maxInt(applied, emitted) < committed) {
      entries.slice(maxInt(applied, emitted), committed)
    } else {
      List()
    }

  pure def appendReq(dst, nextIdx, leaderCommit) = {
    src: LEADER,
    dst: dst,
    req: {
      term: 1,
      leaderId: LEADER,
      prevLogIndex: nextIdx - 1,
      prevLogTerm: termAt(List(noopEntry), nextIdx - 1),
      entries: entriesFrom(List(noopEntry), nextIdx),
      leaderCommit: leaderCommit,
      groupId: GROUP_ID,
    },
  }

  def leaderAppendReq(dst) = {
    src: LEADER,
    dst: dst,
    req: {
      term: currentTerm1,
      leaderId: LEADER,
      prevLogIndex: nextIndexOf(dst) - 1,
      prevLogTerm: termAt(log1, nextIndexOf(dst) - 1),
      entries: entriesFrom(log1, nextIndexOf(dst)),
      leaderCommit: commitIndex1,
      groupId: GROUP_ID,
    },
  }

  def leaderAppendReqFrom(dst, entries) = {
    src: LEADER,
    dst: dst,
    req: {
      term: currentTerm1,
      leaderId: LEADER,
      prevLogIndex: nextIndexOf(dst) - 1,
      prevLogTerm: termAt(entries, nextIndexOf(dst) - 1),
      entries: entriesFrom(entries, nextIndexOf(dst)),
      leaderCommit: commitIndex1,
      groupId: GROUP_ID,
    },
  }

  def leaderAppendReqAt(dst, nextIdx) = {
    src: LEADER,
    dst: dst,
    req: {
      term: currentTerm1,
      leaderId: LEADER,
      prevLogIndex: nextIdx - 1,
      prevLogTerm: termAt(log1, nextIdx - 1),
      entries: entriesFrom(log1, nextIdx),
      leaderCommit: commitIndex1,
      groupId: GROUP_ID,
    },
  }

  pure def appendResp(src, dst, term, success, lastLogIndex) = {
    src: src,
    dst: dst,
    resp: { term: term, success: success, lastLogIndex: lastLogIndex },
  }

  pure def validPrev(entries, req) =
    req.prevLogIndex == 0 or termAt(entries, req.prevLogIndex) == req.prevLogTerm

  pure def retryNextIndex(lastLogIndex, currentNextIndex) =
    if (lastLogIndex + 1 < currentNextIndex) {
      maxInt(lastLogIndex + 1, 1)
    } else {
      maxInt(currentNextIndex - 1, 1)
    }

  def effectiveMatch(peer, ackPeer, ackMatch) =
    if (peer == ackPeer) ackMatch else matchIndexOf(peer)

  def peerQuorumHas(index, ackPeer, ackMatch) =
    effectiveMatch(2, ackPeer, ackMatch) >= index or effectiveMatch(3, ackPeer, ackMatch) >= index

  def commitWithAck(peer, ackMatch, currentCommit) =
    if (log1.length() >= 3 and 3 > currentCommit and termAt(log1, 3) == currentTerm1 and peerQuorumHas(3, peer, ackMatch)) {
      3
    } else if (log1.length() >= 2 and 2 > currentCommit and termAt(log1, 2) == currentTerm1 and peerQuorumHas(2, peer, ackMatch)) {
      2
    } else if (log1.length() >= 1 and 1 > currentCommit and termAt(log1, 1) == currentTerm1 and peerQuorumHas(1, peer, ackMatch)) {
      1
    } else {
      currentCommit
    }

  action init = all {
    role1' = "Leader",
    role2' = "Follower",
    role3' = "Follower",
    currentTerm1' = 3,
    currentTerm2' = 2,
    currentTerm3' = 0,
    leaderId1' = 1,
    leaderId2' = 0,
    leaderId3' = 0,
    commitIndex1' = 0,
    commitIndex2' = 0,
    commitIndex3' = 0,
    lastApplied1' = 0,
    lastApplied2' = 0,
    lastApplied3' = 0,
    readyCommitIndex1' = 0,
    readyCommitIndex2' = 0,
    readyCommitIndex3' = 0,
    log1' = List(noopEntry, leaderEntry2),
    log2' = List(noopEntry, conflictEntry2),
    log3' = List(),
    nextIndex2' = 3,
    nextIndex3' = 1,
    matchIndex2' = 0,
    matchIndex3' = 0,
    appendMessages' = List(
      {
        src: LEADER,
        dst: 2,
        req: {
          term: 3,
          leaderId: LEADER,
          prevLogIndex: 2,
          prevLogTerm: 3,
          entries: List(),
          leaderCommit: 0,
          groupId: GROUP_ID,
        },
      },
      {
        src: LEADER,
        dst: 3,
        req: {
          term: 3,
          leaderId: LEADER,
          prevLogIndex: 0,
          prevLogTerm: 0,
          entries: List(noopEntry, leaderEntry2),
          leaderCommit: 0,
          groupId: GROUP_ID,
        },
      }
    ),
    appendResponses' = List(),
    readyCommitted1' = List(),
    readyCommitted2' = List(),
    readyCommitted3' = List(),
  }

  action ClientPropose = {
    nondet data = COMMANDS.oneOf()
    val entry = { term: currentTerm1, index: log1.length() + 1, data: data }
    val newLog = log1.append(entry)
    all {
      role1 == "Leader",
      log1.length() < MAX_LOG_LENGTH,
      role1' = role1,
      role2' = role2,
      role3' = role3,
      currentTerm1' = currentTerm1,
      currentTerm2' = currentTerm2,
      currentTerm3' = currentTerm3,
      leaderId1' = leaderId1,
      leaderId2' = leaderId2,
      leaderId3' = leaderId3,
      commitIndex1' = commitIndex1,
      commitIndex2' = commitIndex2,
      commitIndex3' = commitIndex3,
      lastApplied1' = lastApplied1,
      lastApplied2' = lastApplied2,
      lastApplied3' = lastApplied3,
      readyCommitIndex1' = readyCommitIndex1,
      readyCommitIndex2' = readyCommitIndex2,
      readyCommitIndex3' = readyCommitIndex3,
      log1' = newLog,
      log2' = log2,
      log3' = log3,
      nextIndex2' = nextIndex2,
      nextIndex3' = nextIndex3,
      matchIndex2' = matchIndex2,
      matchIndex3' = matchIndex3,
      appendMessages' = appendMessages.concat(List(leaderAppendReqFrom(2, newLog), leaderAppendReqFrom(3, newLog))),
      appendResponses' = appendResponses,
      readyCommitted1' = readyCommitted1,
      readyCommitted2' = readyCommitted2,
      readyCommitted3' = readyCommitted3,
    }
  }

  action TickHeartbeat = all {
    role1 == "Leader",
    role1' = role1,
    role2' = role2,
    role3' = role3,
    currentTerm1' = currentTerm1,
    currentTerm2' = currentTerm2,
    currentTerm3' = currentTerm3,
    leaderId1' = leaderId1,
    leaderId2' = leaderId2,
    leaderId3' = leaderId3,
    commitIndex1' = commitIndex1,
    commitIndex2' = commitIndex2,
    commitIndex3' = commitIndex3,
    lastApplied1' = lastApplied1,
    lastApplied2' = lastApplied2,
    lastApplied3' = lastApplied3,
    readyCommitIndex1' = readyCommitIndex1,
    readyCommitIndex2' = readyCommitIndex2,
    readyCommitIndex3' = readyCommitIndex3,
    log1' = log1,
    log2' = log2,
    log3' = log3,
    nextIndex2' = nextIndex2,
    nextIndex3' = nextIndex3,
    matchIndex2' = matchIndex2,
    matchIndex3' = matchIndex3,
    appendMessages' = appendMessages.concat(List(leaderAppendReq(2), leaderAppendReq(3))),
    appendResponses' = appendResponses,
    readyCommitted1' = readyCommitted1,
    readyCommitted2' = readyCommitted2,
    readyCommitted3' = readyCommitted3,
  }

  action HandleAppendEntries = {
    val msg = appendMessages[0]
    val dstLog = logOf(msg.dst)
    val accepted = msg.req.term >= termOf(msg.dst) and validPrev(dstLog, msg.req)
    val newTerm = if (msg.req.term > termOf(msg.dst)) msg.req.term else termOf(msg.dst)
    val newLog = if (accepted) appendEntriesToLog(dstLog, msg.req.prevLogIndex, msg.req.entries) else dstLog
    val newCommit = if (accepted and msg.req.leaderCommit > commitIndexOf(msg.dst)) {
      if (msg.req.leaderCommit <= newLog.length()) msg.req.leaderCommit else newLog.length()
    } else {
      commitIndexOf(msg.dst)
    }
    val newReady = readyCommittedOf(msg.dst).concat(
      committedEntries(newLog, lastAppliedOf(msg.dst), readyCommitIndexOf(msg.dst), newCommit)
    )
    all {
      appendMessages.length() > 0,
      role1' = if (msg.dst == 1 and msg.req.term > currentTerm1) "Follower" else role1,
      role2' = if (msg.dst == 2 and msg.req.term > currentTerm2) "Follower" else role2,
      role3' = if (msg.dst == 3 and msg.req.term > currentTerm3) "Follower" else role3,
      currentTerm1' = if (msg.dst == 1) newTerm else currentTerm1,
      currentTerm2' = if (msg.dst == 2) newTerm else currentTerm2,
      currentTerm3' = if (msg.dst == 3) newTerm else currentTerm3,
      leaderId1' = if (msg.dst == 1 and msg.req.term >= currentTerm1) msg.req.leaderId else leaderId1,
      leaderId2' = if (msg.dst == 2 and msg.req.term >= currentTerm2) msg.req.leaderId else leaderId2,
      leaderId3' = if (msg.dst == 3 and msg.req.term >= currentTerm3) msg.req.leaderId else leaderId3,
      commitIndex1' = if (msg.dst == 1) newCommit else commitIndex1,
      commitIndex2' = if (msg.dst == 2) newCommit else commitIndex2,
      commitIndex3' = if (msg.dst == 3) newCommit else commitIndex3,
      lastApplied1' = lastApplied1,
      lastApplied2' = lastApplied2,
      lastApplied3' = lastApplied3,
      readyCommitIndex1' = if (msg.dst == 1) newCommit else readyCommitIndex1,
      readyCommitIndex2' = if (msg.dst == 2) newCommit else readyCommitIndex2,
      readyCommitIndex3' = if (msg.dst == 3) newCommit else readyCommitIndex3,
      log1' = if (msg.dst == 1) newLog else log1,
      log2' = if (msg.dst == 2) newLog else log2,
      log3' = if (msg.dst == 3) newLog else log3,
      nextIndex2' = nextIndex2,
      nextIndex3' = nextIndex3,
      matchIndex2' = matchIndex2,
      matchIndex3' = matchIndex3,
      appendMessages' = appendMessages.slice(1, appendMessages.length()),
      appendResponses' = appendResponses.append(appendResp(msg.dst, msg.src, newTerm, accepted, newLog.length())),
      readyCommitted1' = if (msg.dst == 1) newReady else readyCommitted1,
      readyCommitted2' = if (msg.dst == 2) newReady else readyCommitted2,
      readyCommitted3' = if (msg.dst == 3) newReady else readyCommitted3,
    }
  }

  action HandleAppendEntriesResponse = {
    val msg = appendResponses[0]
    val stale = msg.resp.term < currentTerm1
    val advancesMatch = not(stale) and msg.resp.success and msg.resp.lastLogIndex > matchIndexOf(msg.src)
    val newMatch = if (advancesMatch) msg.resp.lastLogIndex else matchIndexOf(msg.src)
    val newCommit = if (not(stale) and msg.resp.success) commitWithAck(msg.src, newMatch, commitIndex1) else commitIndex1
    val retryNext = retryNextIndex(msg.resp.lastLogIndex, nextIndexOf(msg.src))
    all {
      appendResponses.length() > 0,
      role1' = role1,
      role2' = role2,
      role3' = role3,
      currentTerm1' = currentTerm1,
      currentTerm2' = currentTerm2,
      currentTerm3' = currentTerm3,
      leaderId1' = leaderId1,
      leaderId2' = leaderId2,
      leaderId3' = leaderId3,
      commitIndex1' = newCommit,
      commitIndex2' = commitIndex2,
      commitIndex3' = commitIndex3,
      lastApplied1' = lastApplied1,
      lastApplied2' = lastApplied2,
      lastApplied3' = lastApplied3,
      readyCommitIndex1' = newCommit,
      readyCommitIndex2' = readyCommitIndex2,
      readyCommitIndex3' = readyCommitIndex3,
      log1' = log1,
      log2' = log2,
      log3' = log3,
      nextIndex2' = if (msg.src == 2 and advancesMatch) newMatch + 1 else if (not(stale) and msg.src == 2 and not(msg.resp.success)) retryNext else nextIndex2,
      nextIndex3' = if (msg.src == 3 and advancesMatch) newMatch + 1 else if (not(stale) and msg.src == 3 and not(msg.resp.success)) retryNext else nextIndex3,
      matchIndex2' = if (msg.src == 2 and advancesMatch) newMatch else matchIndex2,
      matchIndex3' = if (msg.src == 3 and advancesMatch) newMatch else matchIndex3,
      appendMessages' = if (stale or msg.resp.success) appendMessages else appendMessages.append(leaderAppendReqAt(msg.src, retryNext)),
      appendResponses' = appendResponses.slice(1, appendResponses.length()),
      readyCommitted1' = readyCommitted1.concat(committedEntries(log1, lastApplied1, readyCommitIndex1, newCommit)),
      readyCommitted2' = readyCommitted2,
      readyCommitted3' = readyCommitted3,
    }
  }

  action InjectStaleAppendEntriesResponse = all {
    role1 == "Leader",
    currentTerm1 > 0,
    appendResponses.length() < 4,
    role1' = role1,
    role2' = role2,
    role3' = role3,
    currentTerm1' = currentTerm1,
    currentTerm2' = currentTerm2,
    currentTerm3' = currentTerm3,
    leaderId1' = leaderId1,
    leaderId2' = leaderId2,
    leaderId3' = leaderId3,
    commitIndex1' = commitIndex1,
    commitIndex2' = commitIndex2,
    commitIndex3' = commitIndex3,
    lastApplied1' = lastApplied1,
    lastApplied2' = lastApplied2,
    lastApplied3' = lastApplied3,
    readyCommitIndex1' = readyCommitIndex1,
    readyCommitIndex2' = readyCommitIndex2,
    readyCommitIndex3' = readyCommitIndex3,
    log1' = log1,
    log2' = log2,
    log3' = log3,
    nextIndex2' = nextIndex2,
    nextIndex3' = nextIndex3,
    matchIndex2' = matchIndex2,
    matchIndex3' = matchIndex3,
    appendMessages' = appendMessages,
    appendResponses' = appendResponses.append(appendResp(2, 1, currentTerm1 - 1, true, log1.length())),
    readyCommitted1' = readyCommitted1,
    readyCommitted2' = readyCommitted2,
    readyCommitted3' = readyCommitted3,
  }

  action InjectOldTermEntryAppendEntriesResponse = all {
    role1 == "Leader",
    currentTerm1 > 1,
    appendResponses.length() < 4,
    role1' = role1,
    role2' = role2,
    role3' = role3,
    currentTerm1' = currentTerm1,
    currentTerm2' = currentTerm2,
    currentTerm3' = currentTerm3,
    leaderId1' = leaderId1,
    leaderId2' = leaderId2,
    leaderId3' = leaderId3,
    commitIndex1' = commitIndex1,
    commitIndex2' = commitIndex2,
    commitIndex3' = commitIndex3,
    lastApplied1' = lastApplied1,
    lastApplied2' = lastApplied2,
    lastApplied3' = lastApplied3,
    readyCommitIndex1' = readyCommitIndex1,
    readyCommitIndex2' = readyCommitIndex2,
    readyCommitIndex3' = readyCommitIndex3,
    log1' = log1,
    log2' = log2,
    log3' = log3,
    nextIndex2' = nextIndex2,
    nextIndex3' = nextIndex3,
    matchIndex2' = matchIndex2,
    matchIndex3' = matchIndex3,
    appendMessages' = appendMessages,
    appendResponses' = appendResponses.append(appendResp(2, 1, currentTerm1, true, 1)),
    readyCommitted1' = readyCommitted1,
    readyCommitted2' = readyCommitted2,
    readyCommitted3' = readyCommitted3,
  }

  action TakeReady = {
    nondet n = Set(1, 2, 3).oneOf()
    all {
      readyCommittedOf(n).length() > 0,
      role1' = role1,
      role2' = role2,
      role3' = role3,
      currentTerm1' = currentTerm1,
      currentTerm2' = currentTerm2,
      currentTerm3' = currentTerm3,
      leaderId1' = leaderId1,
      leaderId2' = leaderId2,
      leaderId3' = leaderId3,
      commitIndex1' = commitIndex1,
      commitIndex2' = commitIndex2,
      commitIndex3' = commitIndex3,
      lastApplied1' = lastApplied1,
      lastApplied2' = lastApplied2,
      lastApplied3' = lastApplied3,
      readyCommitIndex1' = readyCommitIndex1,
      readyCommitIndex2' = readyCommitIndex2,
      readyCommitIndex3' = readyCommitIndex3,
      log1' = log1,
      log2' = log2,
      log3' = log3,
      nextIndex2' = nextIndex2,
      nextIndex3' = nextIndex3,
      matchIndex2' = matchIndex2,
      matchIndex3' = matchIndex3,
      appendMessages' = appendMessages,
      appendResponses' = appendResponses,
      readyCommitted1' = if (n == 1) List() else readyCommitted1,
      readyCommitted2' = if (n == 2) List() else readyCommitted2,
      readyCommitted3' = if (n == 3) List() else readyCommitted3,
    }
  }

  action AdvanceApplied = {
    nondet n = Set(1, 2, 3).oneOf()
    all {
      lastAppliedOf(n) < commitIndexOf(n),
      readyCommittedOf(n).length() == 0,
      role1' = role1,
      role2' = role2,
      role3' = role3,
      currentTerm1' = currentTerm1,
      currentTerm2' = currentTerm2,
      currentTerm3' = currentTerm3,
      leaderId1' = leaderId1,
      leaderId2' = leaderId2,
      leaderId3' = leaderId3,
      commitIndex1' = commitIndex1,
      commitIndex2' = commitIndex2,
      commitIndex3' = commitIndex3,
      lastApplied1' = if (n == 1) commitIndex1 else lastApplied1,
      lastApplied2' = if (n == 2) commitIndex2 else lastApplied2,
      lastApplied3' = if (n == 3) commitIndex3 else lastApplied3,
      readyCommitIndex1' = readyCommitIndex1,
      readyCommitIndex2' = readyCommitIndex2,
      readyCommitIndex3' = readyCommitIndex3,
      log1' = log1,
      log2' = log2,
      log3' = log3,
      nextIndex2' = nextIndex2,
      nextIndex3' = nextIndex3,
      matchIndex2' = matchIndex2,
      matchIndex3' = matchIndex3,
      appendMessages' = appendMessages,
      appendResponses' = appendResponses,
      readyCommitted1' = readyCommitted1,
      readyCommitted2' = readyCommitted2,
      readyCommitted3' = readyCommitted3,
    }
  }

  action Noop = all {
    role1' = role1,
    role2' = role2,
    role3' = role3,
    currentTerm1' = currentTerm1,
    currentTerm2' = currentTerm2,
    currentTerm3' = currentTerm3,
    leaderId1' = leaderId1,
    leaderId2' = leaderId2,
    leaderId3' = leaderId3,
    commitIndex1' = commitIndex1,
    commitIndex2' = commitIndex2,
    commitIndex3' = commitIndex3,
    lastApplied1' = lastApplied1,
    lastApplied2' = lastApplied2,
    lastApplied3' = lastApplied3,
    readyCommitIndex1' = readyCommitIndex1,
    readyCommitIndex2' = readyCommitIndex2,
    readyCommitIndex3' = readyCommitIndex3,
    log1' = log1,
    log2' = log2,
    log3' = log3,
    nextIndex2' = nextIndex2,
    nextIndex3' = nextIndex3,
    matchIndex2' = matchIndex2,
    matchIndex3' = matchIndex3,
    appendMessages' = appendMessages,
    appendResponses' = appendResponses,
    readyCommitted1' = readyCommitted1,
    readyCommitted2' = readyCommitted2,
    readyCommitted3' = readyCommitted3,
  }

  action step = any {
    ClientPropose,
    TickHeartbeat,
    HandleAppendEntries,
    HandleAppendEntriesResponse,
    InjectStaleAppendEntriesResponse,
    InjectOldTermEntryAppendEntriesResponse,
    TakeReady,
    AdvanceApplied,
    Noop,
  }

  pure def sameEntryAt(a, b, i) =
    i == 0 or i > a.length() or i > b.length() or a[i - 1] == b[i - 1]

  pure def sameTermImpliesSameEntryAt(a, b, i) =
    i == 0 or i > a.length() or i > b.length() or a[i - 1].term != b[i - 1].term or a[i - 1] == b[i - 1]

  pure def prefixesMatchThrough(a, b, i) =
    (i < 1 or sameEntryAt(a, b, 1)) and
      (i < 2 or sameEntryAt(a, b, 2)) and
      (i < 3 or sameEntryAt(a, b, 3))

  val commitWithinLog =
    commitIndex1 <= log1.length() and commitIndex2 <= log2.length() and commitIndex3 <= log3.length()

  val appliedWithinCommit =
    lastApplied1 <= commitIndex1 and lastApplied2 <= commitIndex2 and lastApplied3 <= commitIndex3

  val commitMonotonic =
    commitIndex1 >= 0 and commitIndex2 >= 0 and commitIndex3 >= 0

  val leaderCommitsOnlyCurrentTermEntries =
    commitIndex1 == 0 or termAt(log1, commitIndex1) == currentTerm1

  val logMatching =
    sameTermImpliesSameEntryAt(log1, log2, 1) and sameTermImpliesSameEntryAt(log1, log2, 2) and sameTermImpliesSameEntryAt(log1, log2, 3) and
      sameTermImpliesSameEntryAt(log1, log3, 1) and sameTermImpliesSameEntryAt(log1, log3, 2) and sameTermImpliesSameEntryAt(log1, log3, 3) and
      sameTermImpliesSameEntryAt(log2, log3, 1) and sameTermImpliesSameEntryAt(log2, log3, 2) and sameTermImpliesSameEntryAt(log2, log3, 3)

  val followerCommittedPrefixesMatchLeader =
    prefixesMatchThrough(log1, log2, commitIndex2) and prefixesMatchThrough(log1, log3, commitIndex3)

  val stateMachineSafety =
    sameEntryAt(log1.slice(0, lastApplied1), log2.slice(0, lastApplied2), 1) and
      sameEntryAt(log1.slice(0, lastApplied1), log2.slice(0, lastApplied2), 2) and
      sameEntryAt(log1.slice(0, lastApplied1), log2.slice(0, lastApplied2), 3) and
      sameEntryAt(log1.slice(0, lastApplied1), log3.slice(0, lastApplied3), 1) and
      sameEntryAt(log1.slice(0, lastApplied1), log3.slice(0, lastApplied3), 2) and
      sameEntryAt(log1.slice(0, lastApplied1), log3.slice(0, lastApplied3), 3) and
      sameEntryAt(log2.slice(0, lastApplied2), log3.slice(0, lastApplied3), 1) and
      sameEntryAt(log2.slice(0, lastApplied2), log3.slice(0, lastApplied3), 2) and
      sameEntryAt(log2.slice(0, lastApplied2), log3.slice(0, lastApplied3), 3)
}
