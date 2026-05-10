module LeaderCompleteness {
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
  type RequestVote = {
    term: int,
    candidateId: int,
    lastLogIndex: int,
    lastLogTerm: int,
    groupId: int,
  }
  type RequestVoteResponse = { term: int, voteGranted: bool }
  type AppendEnvelope = { src: int, dst: int, req: AppendEntries }
  type AppendResponseEnvelope = { src: int, dst: int, resp: AppendEntriesResponse }
  type VoteRequestEnvelope = { src: int, dst: int, req: RequestVote }
  type VoteResponseEnvelope = { src: int, dst: int, resp: RequestVoteResponse }

  pure val GROUP_ID = 1
  pure val NODES = Set(1, 2, 3)
  pure val NOOP = 0
  pure val COMMAND = 7
  pure val NO_VOTE = 0
  pure val NO_LEADER = 0
  pure val MAX_TERM = 3

  var role1: str
  var role2: str
  var role3: str
  var currentTerm1: int
  var currentTerm2: int
  var currentTerm3: int
  var votedFor1: int
  var votedFor2: int
  var votedFor3: int
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
  var appendMessages: List[AppendEnvelope]
  var appendResponses: List[AppendResponseEnvelope]
  var voteRequests: List[VoteRequestEnvelope]
  var voteResponses: List[VoteResponseEnvelope]
  var votesReceived1: List[int]
  var votesReceived2: List[int]
  var votesReceived3: List[int]

  pure def noopEntry = { term: 1, index: 1, data: NOOP }
  pure def commandEntry = { term: 1, index: 2, data: COMMAND }

  pure def peers(n) =
    if (n == 1) {
      Set(2, 3)
    } else if (n == 2) {
      Set(1, 3)
    } else {
      Set(1, 2)
    }

  def roleOf(n) =
    if (n == 1) role1 else if (n == 2) role2 else role3

  def termOf(n) =
    if (n == 1) currentTerm1 else if (n == 2) currentTerm2 else currentTerm3

  def votedForOf(n) =
    if (n == 1) votedFor1 else if (n == 2) votedFor2 else votedFor3

  def leaderIdOf(n) =
    if (n == 1) leaderId1 else if (n == 2) leaderId2 else leaderId3

  def commitIndexOf(n) =
    if (n == 1) commitIndex1 else if (n == 2) commitIndex2 else commitIndex3

  def logOf(n) =
    if (n == 1) log1 else if (n == 2) log2 else log3

  def votesReceivedOf(n) =
    if (n == 1) votesReceived1 else if (n == 2) votesReceived2 else votesReceived3

  def nextIndexOf(peer) =
    if (peer == 2) nextIndex2 else nextIndex3

  def matchIndexOf(peer) =
    if (peer == 2) matchIndex2 else matchIndex3

  pure def maxInt(a, b) =
    if (a >= b) a else b

  pure def minInt(a, b) =
    if (a <= b) a else b

  pure def termAt(entries, index) =
    if (index == 0) {
      0
    } else if (index <= entries.length()) {
      entries[index - 1].term
    } else {
      -1
    }

  pure def lastLogIndex(entries) = entries.length()

  pure def lastLogTerm(entries) = termAt(entries, entries.length())

  pure def entriesFrom(entries, fromIndex) =
    if (fromIndex <= entries.length()) {
      entries.slice(fromIndex - 1, entries.length())
    } else {
      List()
    }

  pure def appendEntriesToLog(entries, prevLogIndex, newEntries) =
    entries.slice(0, prevLogIndex).concat(newEntries)

  def appendReq(dst) = {
    src: 1,
    dst: dst,
    req: {
      term: currentTerm1,
      leaderId: 1,
      prevLogIndex: nextIndexOf(dst) - 1,
      prevLogTerm: termAt(log1, nextIndexOf(dst) - 1),
      entries: entriesFrom(log1, nextIndexOf(dst)),
      leaderCommit: commitIndex1,
      groupId: GROUP_ID,
    },
  }

  pure def appendResp(src, dst, term, success, lastLogIndex) = {
    src: src,
    dst: dst,
    resp: { term: term, success: success, lastLogIndex: lastLogIndex },
  }

  pure def voteRequest(src, dst, term, lastIdx, lastTerm) = {
    src: src,
    dst: dst,
    req: {
      term: term,
      candidateId: src,
      lastLogIndex: lastIdx,
      lastLogTerm: lastTerm,
      groupId: GROUP_ID,
    },
  }

  pure def voteResponse(src, dst, term, granted) = {
    src: src,
    dst: dst,
    resp: { term: term, voteGranted: granted },
  }

  pure def validPrev(entries, req) =
    req.prevLogIndex == 0 or termAt(entries, req.prevLogIndex) == req.prevLogTerm

  pure def retryNextIndex(lastIdx, currentNext) =
    if (lastIdx + 1 < currentNext) {
      maxInt(lastIdx + 1, 1)
    } else {
      maxInt(currentNext - 1, 1)
    }

  def peerQuorumHas(index, ackPeer, ackMatch) =
    (ackPeer == 2 and ackMatch >= index) or
      (ackPeer == 3 and ackMatch >= index) or
      matchIndex2 >= index or
      matchIndex3 >= index

  def commitWithAck(ackPeer, ackMatch, currentCommit) =
    if (log1.length() >= 2 and 2 > currentCommit and termAt(log1, 2) == currentTerm1 and peerQuorumHas(2, ackPeer, ackMatch)) {
      2
    } else if (log1.length() >= 1 and 1 > currentCommit and termAt(log1, 1) == currentTerm1 and peerQuorumHas(1, ackPeer, ackMatch)) {
      1
    } else {
      currentCommit
    }

  def upToDate(req, dst) =
    req.lastLogTerm > lastLogTerm(logOf(dst)) or
      (req.lastLogTerm == lastLogTerm(logOf(dst)) and req.lastLogIndex >= lastLogIndex(logOf(dst)))

  def grantVote(req, dst) =
    req.term >= termOf(dst) and
      (req.term > termOf(dst) or votedForOf(dst) == NO_VOTE or votedForOf(dst) == req.candidateId) and
      upToDate(req, dst)

  def newVoteRequests(src) =
    if (src == 2) {
      List(
        voteRequest(2, 1, currentTerm2 + 1, lastLogIndex(log2), lastLogTerm(log2)),
        voteRequest(2, 3, currentTerm2 + 1, lastLogIndex(log2), lastLogTerm(log2))
      )
    } else {
      List(
        voteRequest(3, 1, currentTerm3 + 1, lastLogIndex(log3), lastLogTerm(log3)),
        voteRequest(3, 2, currentTerm3 + 1, lastLogIndex(log3), lastLogTerm(log3))
      )
    }

  pure def wonWith(votes, peer) =
    votes.length() + 2 >= 2

  pure def hasEntry(entries, index) =
    index > 0 and index <= entries.length()

  def someNodeCommitted(index) =
    commitIndex1 >= index or commitIndex2 >= index or commitIndex3 >= index

  def committedEntryAt(index) =
    if (commitIndex1 >= index) {
      log1[index - 1]
    } else if (commitIndex2 >= index) {
      log2[index - 1]
    } else {
      log3[index - 1]
    }

  def matchesCommittedAt(entries, index) =
    not(someNodeCommitted(index)) or (hasEntry(entries, index) and entries[index - 1] == committedEntryAt(index))

  action init = all {
    role1' = "Leader",
    role2' = "Follower",
    role3' = "Follower",
    currentTerm1' = 1,
    currentTerm2' = 1,
    currentTerm3' = 1,
    votedFor1' = 1,
    votedFor2' = NO_VOTE,
    votedFor3' = NO_VOTE,
    leaderId1' = 1,
    leaderId2' = 1,
    leaderId3' = NO_LEADER,
    commitIndex1' = 0,
    commitIndex2' = 0,
    commitIndex3' = 0,
    log1' = List(noopEntry, commandEntry),
    log2' = List(),
    log3' = List(),
    nextIndex2' = 1,
    nextIndex3' = 1,
    matchIndex2' = 0,
    matchIndex3' = 0,
    appendMessages' = List(),
    appendResponses' = List(),
    voteRequests' = List(),
    voteResponses' = List(),
    votesReceived1' = List(),
    votesReceived2' = List(),
    votesReceived3' = List(),
  }

  action TickHeartbeat = all {
    role1 == "Leader",
    role1' = role1,
    role2' = role2,
    role3' = role3,
    currentTerm1' = currentTerm1,
    currentTerm2' = currentTerm2,
    currentTerm3' = currentTerm3,
    votedFor1' = votedFor1,
    votedFor2' = votedFor2,
    votedFor3' = votedFor3,
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
    appendMessages' = appendMessages.concat(List(appendReq(2), appendReq(3))),
    appendResponses' = appendResponses,
    voteRequests' = voteRequests,
    voteResponses' = voteResponses,
    votesReceived1' = votesReceived1,
    votesReceived2' = votesReceived2,
    votesReceived3' = votesReceived3,
  }

  action HandleAppendEntries = {
    val msg = appendMessages[0]
    val dstLog = logOf(msg.dst)
    val accept = msg.req.term >= termOf(msg.dst) and validPrev(dstLog, msg.req)
    val nextLog = if (accept) appendEntriesToLog(dstLog, msg.req.prevLogIndex, msg.req.entries) else dstLog
    val nextCommit = if (accept) minInt(msg.req.leaderCommit, nextLog.length()) else commitIndexOf(msg.dst)
    val nextTerm = if (msg.req.term > termOf(msg.dst)) msg.req.term else termOf(msg.dst)
    all {
      appendMessages.length() > 0,
      role1' = if (msg.dst == 1 and msg.req.term >= currentTerm1) "Follower" else role1,
      role2' = if (msg.dst == 2 and msg.req.term >= currentTerm2) "Follower" else role2,
      role3' = if (msg.dst == 3 and msg.req.term >= currentTerm3) "Follower" else role3,
      currentTerm1' = if (msg.dst == 1) nextTerm else currentTerm1,
      currentTerm2' = if (msg.dst == 2) nextTerm else currentTerm2,
      currentTerm3' = if (msg.dst == 3) nextTerm else currentTerm3,
      votedFor1' = if (msg.dst == 1 and msg.req.term > currentTerm1) NO_VOTE else votedFor1,
      votedFor2' = if (msg.dst == 2 and msg.req.term > currentTerm2) NO_VOTE else votedFor2,
      votedFor3' = if (msg.dst == 3 and msg.req.term > currentTerm3) NO_VOTE else votedFor3,
      leaderId1' = if (msg.dst == 1 and accept) msg.req.leaderId else leaderId1,
      leaderId2' = if (msg.dst == 2 and accept) msg.req.leaderId else leaderId2,
      leaderId3' = if (msg.dst == 3 and accept) msg.req.leaderId else leaderId3,
      commitIndex1' = if (msg.dst == 1) nextCommit else commitIndex1,
      commitIndex2' = if (msg.dst == 2) nextCommit else commitIndex2,
      commitIndex3' = if (msg.dst == 3) nextCommit else commitIndex3,
      log1' = if (msg.dst == 1) nextLog else log1,
      log2' = if (msg.dst == 2) nextLog else log2,
      log3' = if (msg.dst == 3) nextLog else log3,
      nextIndex2' = if (msg.dst == 1 and msg.req.term > currentTerm1) 1 else nextIndex2,
      nextIndex3' = if (msg.dst == 1 and msg.req.term > currentTerm1) 1 else nextIndex3,
      matchIndex2' = if (msg.dst == 1 and msg.req.term > currentTerm1) 0 else matchIndex2,
      matchIndex3' = if (msg.dst == 1 and msg.req.term > currentTerm1) 0 else matchIndex3,
      appendMessages' = appendMessages.slice(1, appendMessages.length()),
      appendResponses' = appendResponses.append(
        appendResp(msg.dst, msg.src, nextTerm, accept, nextLog.length())
      ),
      voteRequests' = voteRequests,
      voteResponses' = voteResponses,
      votesReceived1' = votesReceived1,
      votesReceived2' = votesReceived2,
      votesReceived3' = votesReceived3,
    }
  }

  action HandleAppendEntriesResponse = {
    val msg = appendResponses[0]
    val higherTerm = msg.resp.term > currentTerm1
    val accepted = msg.resp.success and msg.resp.term == currentTerm1
    val ackMatch = if (accepted) msg.resp.lastLogIndex else matchIndexOf(msg.src)
    all {
      appendResponses.length() > 0,
      msg.dst == 1,
      role1 == "Leader",
      role1' = if (higherTerm) "Follower" else role1,
      role2' = role2,
      role3' = role3,
      currentTerm1' = if (higherTerm) msg.resp.term else currentTerm1,
      currentTerm2' = currentTerm2,
      currentTerm3' = currentTerm3,
      votedFor1' = if (higherTerm) NO_VOTE else votedFor1,
      votedFor2' = votedFor2,
      votedFor3' = votedFor3,
      leaderId1' = leaderId1,
      leaderId2' = leaderId2,
      leaderId3' = leaderId3,
      commitIndex1' = if (accepted) commitWithAck(msg.src, ackMatch, commitIndex1) else commitIndex1,
      commitIndex2' = commitIndex2,
      commitIndex3' = commitIndex3,
      log1' = log1,
      log2' = log2,
      log3' = log3,
      nextIndex2' =
        if (higherTerm) 1
        else if (msg.src == 2 and accepted) ackMatch + 1
        else if (msg.src == 2 and not(accepted) and msg.resp.term == currentTerm1) retryNextIndex(msg.resp.lastLogIndex, nextIndex2)
        else nextIndex2,
      nextIndex3' =
        if (higherTerm) 1
        else if (msg.src == 3 and accepted) ackMatch + 1
        else if (msg.src == 3 and not(accepted) and msg.resp.term == currentTerm1) retryNextIndex(msg.resp.lastLogIndex, nextIndex3)
        else nextIndex3,
      matchIndex2' = if (higherTerm) 0 else if (msg.src == 2 and accepted and ackMatch > matchIndex2) ackMatch else matchIndex2,
      matchIndex3' = if (higherTerm) 0 else if (msg.src == 3 and accepted and ackMatch > matchIndex3) ackMatch else matchIndex3,
      appendMessages' = appendMessages,
      appendResponses' = appendResponses.slice(1, appendResponses.length()),
      voteRequests' = voteRequests,
      voteResponses' = voteResponses,
      votesReceived1' = votesReceived1,
      votesReceived2' = votesReceived2,
      votesReceived3' = votesReceived3,
    }
  }

  action TickElectionTimeout = {
    nondet n = Set(2, 3).oneOf()
    all {
      roleOf(n) == "Follower" or roleOf(n) == "Candidate",
      termOf(n) < MAX_TERM,
      role1' = role1,
      role2' = if (n == 2) "Candidate" else role2,
      role3' = if (n == 3) "Candidate" else role3,
      currentTerm1' = currentTerm1,
      currentTerm2' = if (n == 2) currentTerm2 + 1 else currentTerm2,
      currentTerm3' = if (n == 3) currentTerm3 + 1 else currentTerm3,
      votedFor1' = votedFor1,
      votedFor2' = if (n == 2) 2 else votedFor2,
      votedFor3' = if (n == 3) 3 else votedFor3,
      leaderId1' = leaderId1,
      leaderId2' = if (n == 2) NO_LEADER else leaderId2,
      leaderId3' = if (n == 3) NO_LEADER else leaderId3,
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
      appendMessages' = appendMessages,
      appendResponses' = appendResponses,
      voteRequests' = voteRequests.concat(newVoteRequests(n)),
      voteResponses' = voteResponses,
      votesReceived1' = votesReceived1,
      votesReceived2' = if (n == 2) List() else votesReceived2,
      votesReceived3' = if (n == 3) List() else votesReceived3,
    }
  }

  action HandleRequestVote = {
    val msg = voteRequests[0]
    val granted = grantVote(msg.req, msg.dst)
    val nextTerm = if (msg.req.term > termOf(msg.dst)) msg.req.term else termOf(msg.dst)
    all {
      voteRequests.length() > 0,
      role1' = if (msg.dst == 1 and msg.req.term > currentTerm1) "Follower" else role1,
      role2' = if (msg.dst == 2 and msg.req.term > currentTerm2) "Follower" else role2,
      role3' = if (msg.dst == 3 and msg.req.term > currentTerm3) "Follower" else role3,
      currentTerm1' = if (msg.dst == 1) nextTerm else currentTerm1,
      currentTerm2' = if (msg.dst == 2) nextTerm else currentTerm2,
      currentTerm3' = if (msg.dst == 3) nextTerm else currentTerm3,
      votedFor1' = if (msg.dst == 1 and granted) msg.req.candidateId else if (msg.dst == 1 and msg.req.term > currentTerm1) NO_VOTE else votedFor1,
      votedFor2' = if (msg.dst == 2 and granted) msg.req.candidateId else if (msg.dst == 2 and msg.req.term > currentTerm2) NO_VOTE else votedFor2,
      votedFor3' = if (msg.dst == 3 and granted) msg.req.candidateId else if (msg.dst == 3 and msg.req.term > currentTerm3) NO_VOTE else votedFor3,
      leaderId1' = leaderId1,
      leaderId2' = leaderId2,
      leaderId3' = leaderId3,
      commitIndex1' = commitIndex1,
      commitIndex2' = commitIndex2,
      commitIndex3' = commitIndex3,
      log1' = log1,
      log2' = log2,
      log3' = log3,
      nextIndex2' = if (msg.dst == 1 and msg.req.term > currentTerm1) 1 else nextIndex2,
      nextIndex3' = if (msg.dst == 1 and msg.req.term > currentTerm1) 1 else nextIndex3,
      matchIndex2' = if (msg.dst == 1 and msg.req.term > currentTerm1) 0 else matchIndex2,
      matchIndex3' = if (msg.dst == 1 and msg.req.term > currentTerm1) 0 else matchIndex3,
      appendMessages' = appendMessages,
      appendResponses' = appendResponses,
      voteRequests' = voteRequests.slice(1, voteRequests.length()),
      voteResponses' = voteResponses.append(voteResponse(msg.dst, msg.src, nextTerm, granted)),
      votesReceived1' = if (msg.dst == 1 and msg.req.term > currentTerm1) List() else votesReceived1,
      votesReceived2' = if (msg.dst == 2 and msg.req.term > currentTerm2) List() else votesReceived2,
      votesReceived3' = if (msg.dst == 3 and msg.req.term > currentTerm3) List() else votesReceived3,
    }
  }

  action HandleRequestVoteResponse = {
    val msg = voteResponses[0]
    val dstRole = roleOf(msg.dst)
    val dstTerm = termOf(msg.dst)
    val higherTerm = msg.resp.term > dstTerm
    val grantedForCandidate = msg.resp.voteGranted and dstRole == "Candidate" and msg.resp.term == dstTerm
    val win = grantedForCandidate and wonWith(votesReceivedOf(msg.dst), msg.src)
    val nextNoop =
      if (msg.dst == 2) {
        { term: currentTerm2, index: log2.length() + 1, data: NOOP }
      } else {
        { term: currentTerm3, index: log3.length() + 1, data: NOOP }
      }
    all {
      voteResponses.length() > 0,
      role1' = if (msg.dst == 1 and higherTerm) "Follower" else role1,
      role2' = if (msg.dst == 2 and higherTerm) "Follower" else if (msg.dst == 2 and win) "Leader" else role2,
      role3' = if (msg.dst == 3 and higherTerm) "Follower" else if (msg.dst == 3 and win) "Leader" else role3,
      currentTerm1' = if (msg.dst == 1 and higherTerm) msg.resp.term else currentTerm1,
      currentTerm2' = if (msg.dst == 2 and higherTerm) msg.resp.term else currentTerm2,
      currentTerm3' = if (msg.dst == 3 and higherTerm) msg.resp.term else currentTerm3,
      votedFor1' = if (msg.dst == 1 and higherTerm) NO_VOTE else votedFor1,
      votedFor2' = if (msg.dst == 2 and higherTerm) NO_VOTE else votedFor2,
      votedFor3' = if (msg.dst == 3 and higherTerm) NO_VOTE else votedFor3,
      leaderId1' = leaderId1,
      leaderId2' = if (msg.dst == 2 and win) 2 else leaderId2,
      leaderId3' = if (msg.dst == 3 and win) 3 else leaderId3,
      commitIndex1' = commitIndex1,
      commitIndex2' = commitIndex2,
      commitIndex3' = commitIndex3,
      log1' = log1,
      log2' = if (msg.dst == 2 and win) log2.append(nextNoop) else log2,
      log3' = if (msg.dst == 3 and win) log3.append(nextNoop) else log3,
      nextIndex2' = if (msg.dst == 1 and higherTerm) 1 else nextIndex2,
      nextIndex3' = if (msg.dst == 1 and higherTerm) 1 else nextIndex3,
      matchIndex2' = if (msg.dst == 1 and higherTerm) 0 else matchIndex2,
      matchIndex3' = if (msg.dst == 1 and higherTerm) 0 else matchIndex3,
      appendMessages' = appendMessages,
      appendResponses' = appendResponses,
      voteRequests' = voteRequests,
      voteResponses' = voteResponses.slice(1, voteResponses.length()),
      votesReceived1' = if (msg.dst == 1 and higherTerm) List() else votesReceived1,
      votesReceived2' = if (msg.dst == 2 and higherTerm) List() else if (msg.dst == 2 and grantedForCandidate) votesReceived2.append(msg.src) else votesReceived2,
      votesReceived3' = if (msg.dst == 3 and higherTerm) List() else if (msg.dst == 3 and grantedForCandidate) votesReceived3.append(msg.src) else votesReceived3,
    }
  }

  action Noop = all {
    role1' = role1,
    role2' = role2,
    role3' = role3,
    currentTerm1' = currentTerm1,
    currentTerm2' = currentTerm2,
    currentTerm3' = currentTerm3,
    votedFor1' = votedFor1,
    votedFor2' = votedFor2,
    votedFor3' = votedFor3,
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
    appendMessages' = appendMessages,
    appendResponses' = appendResponses,
    voteRequests' = voteRequests,
    voteResponses' = voteResponses,
    votesReceived1' = votesReceived1,
    votesReceived2' = votesReceived2,
    votesReceived3' = votesReceived3,
  }

  action step = any {
    TickHeartbeat,
    HandleAppendEntries,
    HandleAppendEntriesResponse,
    TickElectionTimeout,
    HandleRequestVote,
    HandleRequestVoteResponse,
    Noop,
  }

  val electionSafety =
    Set(1, 2, 3).filter(n => roleOf(n) == "Leader" and termOf(n) == 0).size() <= 1 and
      Set(1, 2, 3).filter(n => roleOf(n) == "Leader" and termOf(n) == 1).size() <= 1 and
      Set(1, 2, 3).filter(n => roleOf(n) == "Leader" and termOf(n) == 2).size() <= 1 and
      Set(1, 2, 3).filter(n => roleOf(n) == "Leader" and termOf(n) == 3).size() <= 1

  val committedEntriesAgree =
    (not(someNodeCommitted(1)) or
      (not(commitIndex1 >= 1) or log1[0] == committedEntryAt(1)) and
      (not(commitIndex2 >= 1) or log2[0] == committedEntryAt(1)) and
      (not(commitIndex3 >= 1) or log3[0] == committedEntryAt(1))) and
      (not(someNodeCommitted(2)) or
        (not(commitIndex1 >= 2) or log1[1] == committedEntryAt(2)) and
        (not(commitIndex2 >= 2) or log2[1] == committedEntryAt(2)) and
        (not(commitIndex3 >= 2) or log3[1] == committedEntryAt(2)))

  val leaderCompleteness =
    committedEntriesAgree and
      (role1 != "Leader" or (matchesCommittedAt(log1, 1) and matchesCommittedAt(log1, 2))) and
      (role2 != "Leader" or (matchesCommittedAt(log2, 1) and matchesCommittedAt(log2, 2))) and
      (role3 != "Leader" or (matchesCommittedAt(log3, 1) and matchesCommittedAt(log3, 2)))

  val candidatesVoteForThemselves =
    (role2 != "Candidate" or votedFor2 == 2) and
      (role3 != "Candidate" or votedFor3 == 3)

  val leaderAppendsNoopInElectionTerm =
    (role2 != "Leader" or termAt(log2, log2.length()) == currentTerm2) and
      (role3 != "Leader" or termAt(log3, log3.length()) == currentTerm3)

  val staleCandidateDenied =
    role3 != "Leader" or matchesCommittedAt(log3, 2)
}
