module SingleGroupElection {
  type LogMeta = { lastLogIndex: int, lastLogTerm: int }
  type RequestVote = {
    term: int,
    candidateId: int,
    lastLogIndex: int,
    lastLogTerm: int,
    groupId: int,
  }
  type RequestVoteResponse = { term: int, voteGranted: bool }
  type VoteRequestEnvelope = { src: int, dst: int, req: RequestVote }
  type VoteResponseEnvelope = { src: int, dst: int, resp: RequestVoteResponse }

  pure val GROUP_ID = 1
  pure val NODES = Set(1, 2, 3)
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
  var lastLogIndex1: int
  var lastLogIndex2: int
  var lastLogIndex3: int
  var lastLogTerm1: int
  var lastLogTerm2: int
  var lastLogTerm3: int
  var votesReceived1: List[int]
  var votesReceived2: List[int]
  var votesReceived3: List[int]
  var voteRequests: List[VoteRequestEnvelope]
  var voteResponses: List[VoteResponseEnvelope]

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

  def lastLogIndexOf(n) =
    if (n == 1) lastLogIndex1 else if (n == 2) lastLogIndex2 else lastLogIndex3

  def lastLogTermOf(n) =
    if (n == 1) lastLogTerm1 else if (n == 2) lastLogTerm2 else lastLogTerm3

  def votesReceivedOf(n) =
    if (n == 1) votesReceived1 else if (n == 2) votesReceived2 else votesReceived3

  def upToDate(req, dst) =
    req.lastLogTerm > lastLogTermOf(dst) or
      (req.lastLogTerm == lastLogTermOf(dst) and req.lastLogIndex >= lastLogIndexOf(dst))

  pure def voteRequest(src, dst, term, lastLogIndex, lastLogTerm) = {
    src: src,
    dst: dst,
    req: {
      term: term,
      candidateId: src,
      lastLogIndex: lastLogIndex,
      lastLogTerm: lastLogTerm,
      groupId: GROUP_ID,
    },
  }

  def newVoteRequests(src) =
    if (src == 1) {
      List(
        voteRequest(1, 2, currentTerm1 + 1, lastLogIndex1, lastLogTerm1),
        voteRequest(1, 3, currentTerm1 + 1, lastLogIndex1, lastLogTerm1)
      )
    } else if (src == 2) {
      List(
        voteRequest(2, 1, currentTerm2 + 1, lastLogIndex2, lastLogTerm2),
        voteRequest(2, 3, currentTerm2 + 1, lastLogIndex2, lastLogTerm2)
      )
    } else {
      List(
        voteRequest(3, 1, currentTerm3 + 1, lastLogIndex3, lastLogTerm3),
        voteRequest(3, 2, currentTerm3 + 1, lastLogIndex3, lastLogTerm3)
      )
    }

  pure def voteResponse(src, dst, term, granted) = {
    src: src,
    dst: dst,
    resp: { term: term, voteGranted: granted },
  }

  def grantVote(req, dst) =
    roleOf(dst) != "Learner" and
      req.term >= termOf(dst) and
      (req.term > termOf(dst) or votedForOf(dst) == NO_VOTE or votedForOf(dst) == req.candidateId) and
      upToDate(req, dst)

  pure def wonWith(votes, peer) =
    votes.length() + 2 >= 2

  action init = all {
    role1' = "Follower",
    role2' = "Follower",
    role3' = "Follower",
    currentTerm1' = 0,
    currentTerm2' = 0,
    currentTerm3' = 0,
    votedFor1' = NO_VOTE,
    votedFor2' = NO_VOTE,
    votedFor3' = NO_VOTE,
    leaderId1' = NO_LEADER,
    leaderId2' = NO_LEADER,
    leaderId3' = NO_LEADER,
    lastLogIndex1' = 0,
    lastLogIndex2' = 0,
    lastLogIndex3' = 0,
    lastLogTerm1' = 0,
    lastLogTerm2' = 0,
    lastLogTerm3' = 0,
    votesReceived1' = List(),
    votesReceived2' = List(),
    votesReceived3' = List(),
    voteRequests' = List(),
    voteResponses' = List(),
  }

  action TickElectionTimeout = {
    nondet n = NODES.oneOf()
    all {
      roleOf(n) == "Follower" or roleOf(n) == "Candidate",
      termOf(n) < MAX_TERM,
      role1' = if (n == 1) "Candidate" else role1,
      role2' = if (n == 2) "Candidate" else role2,
      role3' = if (n == 3) "Candidate" else role3,
      currentTerm1' = if (n == 1) currentTerm1 + 1 else currentTerm1,
      currentTerm2' = if (n == 2) currentTerm2 + 1 else currentTerm2,
      currentTerm3' = if (n == 3) currentTerm3 + 1 else currentTerm3,
      votedFor1' = if (n == 1) n else votedFor1,
      votedFor2' = if (n == 2) n else votedFor2,
      votedFor3' = if (n == 3) n else votedFor3,
      leaderId1' = if (n == 1) NO_LEADER else leaderId1,
      leaderId2' = if (n == 2) NO_LEADER else leaderId2,
      leaderId3' = if (n == 3) NO_LEADER else leaderId3,
      lastLogIndex1' = lastLogIndex1,
      lastLogIndex2' = lastLogIndex2,
      lastLogIndex3' = lastLogIndex3,
      lastLogTerm1' = lastLogTerm1,
      lastLogTerm2' = lastLogTerm2,
      lastLogTerm3' = lastLogTerm3,
      votesReceived1' = if (n == 1) List() else votesReceived1,
      votesReceived2' = if (n == 2) List() else votesReceived2,
      votesReceived3' = if (n == 3) List() else votesReceived3,
      voteRequests' = voteRequests.concat(newVoteRequests(n)),
      voteResponses' = voteResponses,
    }
  }

  action HandleRequestVote = {
    val msg = voteRequests[0]
    val granted = grantVote(msg.req, msg.dst)
    val newTerm = if (msg.req.term > termOf(msg.dst)) msg.req.term else termOf(msg.dst)
    all {
      voteRequests.length() > 0,
      role1' =
        if (msg.dst == 1 and msg.req.term > currentTerm1) "Follower" else role1,
      role2' =
        if (msg.dst == 2 and msg.req.term > currentTerm2) "Follower" else role2,
      role3' =
        if (msg.dst == 3 and msg.req.term > currentTerm3) "Follower" else role3,
      currentTerm1' = if (msg.dst == 1) newTerm else currentTerm1,
      currentTerm2' = if (msg.dst == 2) newTerm else currentTerm2,
      currentTerm3' = if (msg.dst == 3) newTerm else currentTerm3,
      votedFor1' = if (msg.dst == 1 and granted) msg.req.candidateId else if (msg.dst == 1 and msg.req.term > currentTerm1) NO_VOTE else votedFor1,
      votedFor2' = if (msg.dst == 2 and granted) msg.req.candidateId else if (msg.dst == 2 and msg.req.term > currentTerm2) NO_VOTE else votedFor2,
      votedFor3' = if (msg.dst == 3 and granted) msg.req.candidateId else if (msg.dst == 3 and msg.req.term > currentTerm3) NO_VOTE else votedFor3,
      leaderId1' = leaderId1,
      leaderId2' = leaderId2,
      leaderId3' = leaderId3,
      lastLogIndex1' = lastLogIndex1,
      lastLogIndex2' = lastLogIndex2,
      lastLogIndex3' = lastLogIndex3,
      lastLogTerm1' = lastLogTerm1,
      lastLogTerm2' = lastLogTerm2,
      lastLogTerm3' = lastLogTerm3,
      votesReceived1' = if (msg.dst == 1 and msg.req.term > currentTerm1) List() else votesReceived1,
      votesReceived2' = if (msg.dst == 2 and msg.req.term > currentTerm2) List() else votesReceived2,
      votesReceived3' = if (msg.dst == 3 and msg.req.term > currentTerm3) List() else votesReceived3,
      voteRequests' = voteRequests.slice(1, voteRequests.length()),
      voteResponses' = voteResponses.append(voteResponse(msg.dst, msg.src, newTerm, granted)),
    }
  }

  action HandleRequestVoteResponse = {
    val msg = voteResponses[0]
    val dstRole = roleOf(msg.dst)
    val dstTerm = termOf(msg.dst)
    val higherTerm = msg.resp.term > dstTerm
    val grantedForCandidate = msg.resp.voteGranted and dstRole == "Candidate" and msg.resp.term == dstTerm
    val win = grantedForCandidate and wonWith(votesReceivedOf(msg.dst), msg.src)
    all {
      voteResponses.length() > 0,
      role1' = if (msg.dst == 1 and higherTerm) "Follower" else if (msg.dst == 1 and win) "Leader" else role1,
      role2' = if (msg.dst == 2 and higherTerm) "Follower" else if (msg.dst == 2 and win) "Leader" else role2,
      role3' = if (msg.dst == 3 and higherTerm) "Follower" else if (msg.dst == 3 and win) "Leader" else role3,
      currentTerm1' = if (msg.dst == 1 and higherTerm) msg.resp.term else currentTerm1,
      currentTerm2' = if (msg.dst == 2 and higherTerm) msg.resp.term else currentTerm2,
      currentTerm3' = if (msg.dst == 3 and higherTerm) msg.resp.term else currentTerm3,
      votedFor1' = if (msg.dst == 1 and higherTerm) NO_VOTE else votedFor1,
      votedFor2' = if (msg.dst == 2 and higherTerm) NO_VOTE else votedFor2,
      votedFor3' = if (msg.dst == 3 and higherTerm) NO_VOTE else votedFor3,
      leaderId1' = if (msg.dst == 1 and win) msg.dst else leaderId1,
      leaderId2' = if (msg.dst == 2 and win) msg.dst else leaderId2,
      leaderId3' = if (msg.dst == 3 and win) msg.dst else leaderId3,
      lastLogIndex1' = if (msg.dst == 1 and win) lastLogIndex1 + 1 else lastLogIndex1,
      lastLogIndex2' = if (msg.dst == 2 and win) lastLogIndex2 + 1 else lastLogIndex2,
      lastLogIndex3' = if (msg.dst == 3 and win) lastLogIndex3 + 1 else lastLogIndex3,
      lastLogTerm1' = if (msg.dst == 1 and win) currentTerm1 else lastLogTerm1,
      lastLogTerm2' = if (msg.dst == 2 and win) currentTerm2 else lastLogTerm2,
      lastLogTerm3' = if (msg.dst == 3 and win) currentTerm3 else lastLogTerm3,
      votesReceived1' = if (msg.dst == 1 and higherTerm) List() else if (msg.dst == 1 and grantedForCandidate) votesReceived1.append(msg.src) else votesReceived1,
      votesReceived2' = if (msg.dst == 2 and higherTerm) List() else if (msg.dst == 2 and grantedForCandidate) votesReceived2.append(msg.src) else votesReceived2,
      votesReceived3' = if (msg.dst == 3 and higherTerm) List() else if (msg.dst == 3 and grantedForCandidate) votesReceived3.append(msg.src) else votesReceived3,
      voteRequests' = voteRequests,
      voteResponses' = voteResponses.slice(1, voteResponses.length()),
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
    lastLogIndex1' = lastLogIndex1,
    lastLogIndex2' = lastLogIndex2,
    lastLogIndex3' = lastLogIndex3,
    lastLogTerm1' = lastLogTerm1,
    lastLogTerm2' = lastLogTerm2,
    lastLogTerm3' = lastLogTerm3,
    votesReceived1' = votesReceived1,
    votesReceived2' = votesReceived2,
    votesReceived3' = votesReceived3,
    voteRequests' = voteRequests,
    voteResponses' = voteResponses,
  }

  action step = any {
    TickElectionTimeout,
    HandleRequestVote,
    HandleRequestVoteResponse,
    Noop,
  }

  val termMonotonic = currentTerm1 >= 0 and currentTerm2 >= 0 and currentTerm3 >= 0

  def leadersInTerm(t) =
    Set(1, 2, 3).filter(n => roleOf(n) == "Leader" and termOf(n) == t)

  val electionSafety =
    leadersInTerm(0).size() <= 1 and
      leadersInTerm(1).size() <= 1 and
      leadersInTerm(2).size() <= 1 and
      leadersInTerm(3).size() <= 1

  val oneVotePerTerm =
    (votedFor1 == NO_VOTE or NODES.contains(votedFor1)) and
      (votedFor2 == NO_VOTE or NODES.contains(votedFor2)) and
      (votedFor3 == NO_VOTE or NODES.contains(votedFor3))

  val leaderAppendsNoopInElectionTerm =
    (role1 != "Leader" or (lastLogIndex1 > 0 and lastLogTerm1 == currentTerm1)) and
      (role2 != "Leader" or (lastLogIndex2 > 0 and lastLogTerm2 == currentTerm2)) and
      (role3 != "Leader" or (lastLogIndex3 > 0 and lastLogTerm3 == currentTerm3))

  val candidatesVoteForThemselves =
    (role1 != "Candidate" or votedFor1 == 1) and
      (role2 != "Candidate" or votedFor2 == 2) and
      (role3 != "Candidate" or votedFor3 == 3)
}
