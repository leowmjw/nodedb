module Learners {
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
  pure val VOTER = 2
  pure val LEARNER = 3
  pure val NOOP = 0
  pure val COMMAND = 7
  pure val MAX_LOG_LENGTH = 2

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
  var log1: List[LogEntry]
  var log2: List[LogEntry]
  var log3: List[LogEntry]
  var nextIndex2: int
  var nextIndex3: int
  var matchIndex2: int
  var matchIndex3: int
  var learner3: bool
  var voter3: bool
  var promoted3: bool
  var startsAsLearner3: bool
  var learnerVoteGranted: bool
  var appendMessages: List[AppendEnvelope]
  var appendResponses: List[AppendResponseEnvelope]

  pure def noopEntry = { term: 1, index: 1, data: NOOP }

  def roleOf(n) =
    if (n == 1) role1 else if (n == 2) role2 else role3

  def termOf(n) =
    if (n == 1) currentTerm1 else if (n == 2) currentTerm2 else currentTerm3

  def commitIndexOf(n) =
    if (n == 1) commitIndex1 else if (n == 2) commitIndex2 else commitIndex3

  def logOf(n) =
    if (n == 1) log1 else if (n == 2) log2 else log3

  def nextIndexOf(peer) =
    if (peer == 2) nextIndex2 else nextIndex3

  def matchIndexOf(peer) =
    if (peer == 2) matchIndex2 else matchIndex3

  def peerIsVoter(peer) =
    peer == VOTER or (peer == LEARNER and voter3)

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

  pure def appendResp(src, dst, term, success, lastLogIndex) = {
    src: src,
    dst: dst,
    resp: { term: term, success: success, lastLogIndex: lastLogIndex },
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

  pure def validPrev(entries, req) =
    req.prevLogIndex == 0 or termAt(entries, req.prevLogIndex) == req.prevLogTerm

  pure def maxInt(a, b) =
    if (a >= b) a else b

  pure def retryNextIndex(lastLogIndex, currentNextIndex) =
    if (lastLogIndex + 1 < currentNextIndex) {
      maxInt(lastLogIndex + 1, 1)
    } else {
      maxInt(currentNextIndex - 1, 1)
    }

  def effectiveMatch(peer, ackPeer, ackMatch) =
    if (peer == ackPeer) ackMatch else matchIndexOf(peer)

  def commitWithAck(peer, ackMatch, currentCommit) =
    if (peerIsVoter(peer) and log1.length() >= 2 and 2 > currentCommit and termAt(log1, 2) == currentTerm1 and effectiveMatch(peer, peer, ackMatch) >= 2) {
      2
    } else {
      currentCommit
    }

  action init = all {
    role1' = "Leader",
    role2' = "Follower",
    role3' = "Learner",
    currentTerm1' = 1,
    currentTerm2' = 1,
    currentTerm3' = 1,
    leaderId1' = 1,
    leaderId2' = 1,
    leaderId3' = 0,
    commitIndex1' = 1,
    commitIndex2' = 1,
    commitIndex3' = 0,
    log1' = List(noopEntry),
    log2' = List(noopEntry),
    log3' = List(),
    nextIndex2' = 2,
    nextIndex3' = 1,
    matchIndex2' = 1,
    matchIndex3' = 0,
    learner3' = false,
    voter3' = false,
    promoted3' = false,
    startsAsLearner3' = true,
    learnerVoteGranted' = false,
    appendMessages' = List(),
    appendResponses' = List(),
  }

  action AddLearner = all {
    role1 == "Leader",
    not(learner3),
    not(voter3),
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
    log1' = log1,
    log2' = log2,
    log3' = log3,
    nextIndex2' = nextIndex2,
    nextIndex3' = log1.length() + 1,
    matchIndex2' = matchIndex2,
    matchIndex3' = 0,
    learner3' = true,
    voter3' = voter3,
    promoted3' = false,
    startsAsLearner3' = startsAsLearner3,
    learnerVoteGranted' = learnerVoteGranted,
    appendMessages' = appendMessages,
    appendResponses' = appendResponses,
  }

  action AddVoter = all {
    role1 == "Leader",
    not(learner3),
    not(voter3),
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
    log1' = log1,
    log2' = log2,
    log3' = log3,
    nextIndex2' = nextIndex2,
    nextIndex3' = log1.length() + 1,
    matchIndex2' = matchIndex2,
    matchIndex3' = 0,
    learner3' = false,
    voter3' = true,
    promoted3' = false,
    startsAsLearner3' = startsAsLearner3,
    learnerVoteGranted' = learnerVoteGranted,
    appendMessages' = appendMessages,
    appendResponses' = appendResponses,
  }

  action RemoveLearner = all {
    learner3,
    not(voter3),
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
    log1' = log1,
    log2' = log2,
    log3' = log3,
    nextIndex2' = nextIndex2,
    nextIndex3' = 1,
    matchIndex2' = matchIndex2,
    matchIndex3' = 0,
    learner3' = false,
    voter3' = false,
    promoted3' = false,
    startsAsLearner3' = startsAsLearner3,
    learnerVoteGranted' = learnerVoteGranted,
    appendMessages' = appendMessages,
    appendResponses' = appendResponses,
  }

  action RemoveVoter = all {
    voter3,
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
    log1' = log1,
    log2' = log2,
    log3' = log3,
    nextIndex2' = nextIndex2,
    nextIndex3' = 1,
    matchIndex2' = matchIndex2,
    matchIndex3' = 0,
    learner3' = false,
    voter3' = false,
    promoted3' = false,
    startsAsLearner3' = startsAsLearner3,
    learnerVoteGranted' = learnerVoteGranted,
    appendMessages' = appendMessages,
    appendResponses' = appendResponses,
  }

  action ClientPropose = {
    val entry = { term: currentTerm1, index: log1.length() + 1, data: COMMAND }
    val newLog = log1.append(entry)
    val tracked = if (learner3 or voter3) {
      List(leaderAppendReqFrom(2, newLog), {
        src: LEADER,
        dst: LEARNER,
        req: {
          term: currentTerm1,
          leaderId: LEADER,
          prevLogIndex: nextIndex3 - 1,
          prevLogTerm: termAt(newLog, nextIndex3 - 1),
          entries: entriesFrom(newLog, nextIndex3),
          leaderCommit: commitIndex1,
          groupId: GROUP_ID,
        },
      })
    } else {
      List(leaderAppendReqFrom(2, newLog))
    }
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
      log1' = newLog,
      log2' = log2,
      log3' = log3,
      nextIndex2' = nextIndex2,
      nextIndex3' = nextIndex3,
      matchIndex2' = matchIndex2,
      matchIndex3' = matchIndex3,
      learner3' = learner3,
      voter3' = voter3,
      promoted3' = promoted3,
      startsAsLearner3' = startsAsLearner3,
      learnerVoteGranted' = learnerVoteGranted,
      appendMessages' = appendMessages.concat(tracked),
      appendResponses' = appendResponses,
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
    log1' = log1,
    log2' = log2,
    log3' = log3,
    nextIndex2' = nextIndex2,
    nextIndex3' = nextIndex3,
    matchIndex2' = matchIndex2,
    matchIndex3' = matchIndex3,
    learner3' = learner3,
    voter3' = voter3,
    promoted3' = promoted3,
    startsAsLearner3' = startsAsLearner3,
    learnerVoteGranted' = learnerVoteGranted,
    appendMessages' = if (learner3 or voter3) {
      appendMessages.concat(List(leaderAppendReq(2), leaderAppendReq(3)))
    } else {
      appendMessages.append(leaderAppendReq(2))
    },
    appendResponses' = appendResponses,
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
    all {
      appendMessages.length() > 0,
      role1' = role1,
      role2' = role2,
      role3' = if (msg.dst == 3 and startsAsLearner3) "Learner" else role3,
      currentTerm1' = if (msg.dst == 1) newTerm else currentTerm1,
      currentTerm2' = if (msg.dst == 2) newTerm else currentTerm2,
      currentTerm3' = if (msg.dst == 3) newTerm else currentTerm3,
      leaderId1' = leaderId1,
      leaderId2' = if (msg.dst == 2 and msg.req.term >= currentTerm2) msg.req.leaderId else leaderId2,
      leaderId3' = if (msg.dst == 3 and msg.req.term >= currentTerm3) msg.req.leaderId else leaderId3,
      commitIndex1' = commitIndex1,
      commitIndex2' = if (msg.dst == 2) newCommit else commitIndex2,
      commitIndex3' = if (msg.dst == 3) newCommit else commitIndex3,
      log1' = log1,
      log2' = if (msg.dst == 2) newLog else log2,
      log3' = if (msg.dst == 3) newLog else log3,
      nextIndex2' = nextIndex2,
      nextIndex3' = nextIndex3,
      matchIndex2' = matchIndex2,
      matchIndex3' = matchIndex3,
      learner3' = learner3,
      voter3' = voter3,
      promoted3' = promoted3,
      startsAsLearner3' = startsAsLearner3,
      learnerVoteGranted' = learnerVoteGranted,
      appendMessages' = appendMessages.slice(1, appendMessages.length()),
      appendResponses' = appendResponses.append(appendResp(msg.dst, msg.src, newTerm, accepted, newLog.length())),
    }
  }

  action HandleAppendEntriesResponse = {
    val msg = appendResponses[0]
    val peerTracked = msg.src == 2 or learner3 or voter3
    val advancesMatch = msg.resp.success and peerTracked and msg.resp.lastLogIndex > matchIndexOf(msg.src)
    val newMatch = if (advancesMatch) msg.resp.lastLogIndex else matchIndexOf(msg.src)
    val newCommit = if (msg.resp.success) commitWithAck(msg.src, newMatch, commitIndex1) else commitIndex1
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
      log1' = log1,
      log2' = log2,
      log3' = log3,
      nextIndex2' = if (msg.src == 2 and advancesMatch) newMatch + 1 else if (msg.src == 2 and not(msg.resp.success)) retryNext else nextIndex2,
      nextIndex3' = if (msg.src == 3 and advancesMatch) newMatch + 1 else if (msg.src == 3 and not(msg.resp.success) and peerTracked) retryNext else nextIndex3,
      matchIndex2' = if (msg.src == 2 and advancesMatch) newMatch else matchIndex2,
      matchIndex3' = if (msg.src == 3 and advancesMatch) newMatch else matchIndex3,
      learner3' = learner3,
      voter3' = voter3,
      promoted3' = promoted3,
      startsAsLearner3' = startsAsLearner3,
      learnerVoteGranted' = learnerVoteGranted,
      appendMessages' = if (msg.resp.success) appendMessages else appendMessages.append(leaderAppendReqAt(msg.src, retryNext)),
      appendResponses' = appendResponses.slice(1, appendResponses.length()),
    }
  }

  action RequestVoteAtLearner = all {
    role3 == "Learner",
    not(voter3),
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
    log1' = log1,
    log2' = log2,
    log3' = log3,
    nextIndex2' = nextIndex2,
    nextIndex3' = nextIndex3,
    matchIndex2' = matchIndex2,
    matchIndex3' = matchIndex3,
    learner3' = learner3,
    voter3' = voter3,
    promoted3' = promoted3,
    startsAsLearner3' = startsAsLearner3,
    learnerVoteGranted' = false,
    appendMessages' = appendMessages,
    appendResponses' = appendResponses,
  }

  action TickLearnerElectionTimeout = all {
    role3 == "Learner",
    not(voter3),
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
    log1' = log1,
    log2' = log2,
    log3' = log3,
    nextIndex2' = nextIndex2,
    nextIndex3' = nextIndex3,
    matchIndex2' = matchIndex2,
    matchIndex3' = matchIndex3,
    learner3' = learner3,
    voter3' = voter3,
    promoted3' = promoted3,
    startsAsLearner3' = startsAsLearner3,
    learnerVoteGranted' = learnerVoteGranted,
    appendMessages' = appendMessages,
    appendResponses' = appendResponses,
  }

  action PromoteLearner = all {
    learner3,
    not(voter3),
    matchIndex3 >= commitIndex1,
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
    log1' = log1,
    log2' = log2,
    log3' = log3,
    nextIndex2' = nextIndex2,
    nextIndex3' = nextIndex3,
    matchIndex2' = matchIndex2,
    matchIndex3' = matchIndex3,
    learner3' = false,
    voter3' = true,
    promoted3' = true,
    startsAsLearner3' = startsAsLearner3,
    learnerVoteGranted' = learnerVoteGranted,
    appendMessages' = appendMessages,
    appendResponses' = appendResponses,
  }

  action PromoteSelf = all {
    role3 == "Learner",
    role1' = role1,
    role2' = role2,
    role3' = "Follower",
    currentTerm1' = currentTerm1,
    currentTerm2' = currentTerm2,
    currentTerm3' = currentTerm3,
    leaderId1' = leaderId1,
    leaderId2' = leaderId2,
    leaderId3' = leaderId3,
    commitIndex1' = commitIndex1,
    commitIndex2' = commitIndex2,
    commitIndex3' = commitIndex3,
    log1' = log1,
    log2' = log2,
    log3' = log3,
    nextIndex2' = nextIndex2,
    nextIndex3' = nextIndex3,
    matchIndex2' = matchIndex2,
    matchIndex3' = matchIndex3,
    learner3' = learner3,
    voter3' = voter3,
    promoted3' = promoted3,
    startsAsLearner3' = false,
    learnerVoteGranted' = learnerVoteGranted,
    appendMessages' = appendMessages,
    appendResponses' = appendResponses,
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
    log1' = log1,
    log2' = log2,
    log3' = log3,
    nextIndex2' = nextIndex2,
    nextIndex3' = nextIndex3,
    matchIndex2' = matchIndex2,
    matchIndex3' = matchIndex3,
    learner3' = learner3,
    voter3' = voter3,
    promoted3' = promoted3,
    startsAsLearner3' = startsAsLearner3,
    learnerVoteGranted' = learnerVoteGranted,
    appendMessages' = appendMessages,
    appendResponses' = appendResponses,
  }

  val quorum1 = 2
  val clusterSize1 = if (voter3) 3 else 2

  val learnerExcludedFromQuorum = not(learner3 and not(voter3)) or (quorum1 == 2 and clusterSize1 == 2)

  val learnerCannotLead = role3 != "Leader"

  val learnerDoesNotVote = not(learnerVoteGranted)

  val learnerAckDoesNotCommit = voter3 or commitIndex1 <= 1 or matchIndex2 >= commitIndex1

  val promotionOnlyAfterCatchUp = not(promoted3) or matchIndex3 >= 1

  val membershipTrackingMatchesConfig =
    if (learner3 or voter3)
      nextIndex3 >= 1
    else
      nextIndex3 == 1 and matchIndex3 == 0

  val directVoterChangesClusterSize =
    if (voter3)
      clusterSize1 == 3 and quorum1 == 2
    else
      clusterSize1 == 2 and quorum1 == 2

  val logMatching = all {
    log1.indices().forall(i => i >= log2.length() or log1[i].index != log2[i].index or log1[i].term != log2[i].term or log1.slice(0, i + 1) == log2.slice(0, i + 1)),
    log1.indices().forall(i => i >= log3.length() or log1[i].index != log3[i].index or log1[i].term != log3[i].term or log1.slice(0, i + 1) == log3.slice(0, i + 1)),
  }

  action step = any {
    AddLearner,
    AddVoter,
    RemoveLearner,
    RemoveVoter,
    ClientPropose,
    TickHeartbeat,
    HandleAppendEntries,
    HandleAppendEntriesResponse,
    RequestVoteAtLearner,
    TickLearnerElectionTimeout,
    PromoteLearner,
    PromoteSelf,
    Noop,
  }
}
