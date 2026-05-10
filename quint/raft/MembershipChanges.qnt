module MembershipChanges {
  pure val LAST_LOG_INDEX = 2

  var role1: str
  var leaderId1: int
  var currentTerm1: int
  var commitIndex1: int
  var voter2: bool
  var voter3: bool
  var learner3: bool
  var clusterSize1: int
  var quorum1: int
  var tracked2: bool
  var tracked3: bool
  var nextIndex2: int
  var nextIndex3: int
  var matchIndex2: int
  var matchIndex3: int
  var heartbeatTargets: List[int]

  pure def boolToInt(b) =
    if (b) 1 else 0

  pure def expectedClusterSize(v2, v3) =
    1 + boolToInt(v2) + boolToInt(v3)

  pure def expectedQuorum(v2, v3) =
    expectedClusterSize(v2, v3) / 2 + 1

  pure def expectedTargets(v2, v3, l3) =
    if (v2 and (v3 or l3)) {
      List(2, 3)
    } else if (v2) {
      List(2)
    } else if (v3 or l3) {
      List(3)
    } else {
      List()
    }

  action init = all {
    role1' = "Leader",
    leaderId1' = 1,
    currentTerm1' = 1,
    commitIndex1' = LAST_LOG_INDEX,
    voter2' = false,
    voter3' = false,
    learner3' = false,
    clusterSize1' = 1,
    quorum1' = 1,
    tracked2' = false,
    tracked3' = false,
    nextIndex2' = 1,
    nextIndex3' = 1,
    matchIndex2' = 0,
    matchIndex3' = 0,
    heartbeatTargets' = List(),
  }

  action AddPeer2 = all {
    role1 == "Leader",
    not(voter2),
    role1' = role1,
    leaderId1' = leaderId1,
    currentTerm1' = currentTerm1,
    commitIndex1' = commitIndex1,
    voter2' = true,
    voter3' = voter3,
    learner3' = learner3,
    clusterSize1' = expectedClusterSize(true, voter3),
    quorum1' = expectedQuorum(true, voter3),
    tracked2' = true,
    tracked3' = tracked3,
    nextIndex2' = LAST_LOG_INDEX + 1,
    nextIndex3' = nextIndex3,
    matchIndex2' = 0,
    matchIndex3' = matchIndex3,
    heartbeatTargets' = List(),
  }

  action AddPeer3 = all {
    role1 == "Leader",
    not(voter3),
    not(learner3),
    role1' = role1,
    leaderId1' = leaderId1,
    currentTerm1' = currentTerm1,
    commitIndex1' = commitIndex1,
    voter2' = voter2,
    voter3' = true,
    learner3' = false,
    clusterSize1' = expectedClusterSize(voter2, true),
    quorum1' = expectedQuorum(voter2, true),
    tracked2' = tracked2,
    tracked3' = true,
    nextIndex2' = nextIndex2,
    nextIndex3' = LAST_LOG_INDEX + 1,
    matchIndex2' = matchIndex2,
    matchIndex3' = 0,
    heartbeatTargets' = List(),
  }

  action AddLearner3 = all {
    role1 == "Leader",
    not(voter3),
    not(learner3),
    role1' = role1,
    leaderId1' = leaderId1,
    currentTerm1' = currentTerm1,
    commitIndex1' = commitIndex1,
    voter2' = voter2,
    voter3' = false,
    learner3' = true,
    clusterSize1' = expectedClusterSize(voter2, false),
    quorum1' = expectedQuorum(voter2, false),
    tracked2' = tracked2,
    tracked3' = true,
    nextIndex2' = nextIndex2,
    nextIndex3' = LAST_LOG_INDEX + 1,
    matchIndex2' = matchIndex2,
    matchIndex3' = 0,
    heartbeatTargets' = List(),
  }

  action RemovePeer2 = all {
    role1 == "Leader",
    voter2,
    role1' = role1,
    leaderId1' = leaderId1,
    currentTerm1' = currentTerm1,
    commitIndex1' = commitIndex1,
    voter2' = false,
    voter3' = voter3,
    learner3' = learner3,
    clusterSize1' = expectedClusterSize(false, voter3),
    quorum1' = expectedQuorum(false, voter3),
    tracked2' = false,
    tracked3' = tracked3,
    nextIndex2' = 1,
    nextIndex3' = nextIndex3,
    matchIndex2' = 0,
    matchIndex3' = matchIndex3,
    heartbeatTargets' = List(),
  }

  action RemovePeer3 = all {
    role1 == "Leader",
    voter3,
    role1' = role1,
    leaderId1' = leaderId1,
    currentTerm1' = currentTerm1,
    commitIndex1' = commitIndex1,
    voter2' = voter2,
    voter3' = false,
    learner3' = false,
    clusterSize1' = expectedClusterSize(voter2, false),
    quorum1' = expectedQuorum(voter2, false),
    tracked2' = tracked2,
    tracked3' = false,
    nextIndex2' = nextIndex2,
    nextIndex3' = 1,
    matchIndex2' = matchIndex2,
    matchIndex3' = 0,
    heartbeatTargets' = List(),
  }

  action RemoveLearner3 = all {
    role1 == "Leader",
    learner3,
    not(voter3),
    role1' = role1,
    leaderId1' = leaderId1,
    currentTerm1' = currentTerm1,
    commitIndex1' = commitIndex1,
    voter2' = voter2,
    voter3' = false,
    learner3' = false,
    clusterSize1' = expectedClusterSize(voter2, false),
    quorum1' = expectedQuorum(voter2, false),
    tracked2' = tracked2,
    tracked3' = false,
    nextIndex2' = nextIndex2,
    nextIndex3' = 1,
    matchIndex2' = matchIndex2,
    matchIndex3' = 0,
    heartbeatTargets' = List(),
  }

  action SetVoters2 = all {
    role1 == "Leader",
    role1' = role1,
    leaderId1' = leaderId1,
    currentTerm1' = currentTerm1,
    commitIndex1' = commitIndex1,
    voter2' = true,
    voter3' = false,
    learner3' = learner3,
    clusterSize1' = expectedClusterSize(true, false),
    quorum1' = expectedQuorum(true, false),
    tracked2' = true,
    tracked3' = learner3,
    nextIndex2' = LAST_LOG_INDEX + 1,
    nextIndex3' = if (learner3) LAST_LOG_INDEX + 1 else 1,
    matchIndex2' = 0,
    matchIndex3' = if (learner3) matchIndex3 else 0,
    heartbeatTargets' = List(),
  }

  action SetVoters23 = all {
    role1 == "Leader",
    not(learner3),
    role1' = role1,
    leaderId1' = leaderId1,
    currentTerm1' = currentTerm1,
    commitIndex1' = commitIndex1,
    voter2' = true,
    voter3' = true,
    learner3' = false,
    clusterSize1' = expectedClusterSize(true, true),
    quorum1' = expectedQuorum(true, true),
    tracked2' = true,
    tracked3' = true,
    nextIndex2' = LAST_LOG_INDEX + 1,
    nextIndex3' = LAST_LOG_INDEX + 1,
    matchIndex2' = 0,
    matchIndex3' = 0,
    heartbeatTargets' = List(),
  }

  action SetVoters3 = all {
    role1 == "Leader",
    not(learner3),
    role1' = role1,
    leaderId1' = leaderId1,
    currentTerm1' = currentTerm1,
    commitIndex1' = commitIndex1,
    voter2' = false,
    voter3' = true,
    learner3' = false,
    clusterSize1' = expectedClusterSize(false, true),
    quorum1' = expectedQuorum(false, true),
    tracked2' = false,
    tracked3' = true,
    nextIndex2' = 1,
    nextIndex3' = LAST_LOG_INDEX + 1,
    matchIndex2' = 0,
    matchIndex3' = 0,
    heartbeatTargets' = List(),
  }

  action TickHeartbeat = all {
    role1 == "Leader",
    role1' = role1,
    leaderId1' = leaderId1,
    currentTerm1' = currentTerm1,
    commitIndex1' = commitIndex1,
    voter2' = voter2,
    voter3' = voter3,
    learner3' = learner3,
    clusterSize1' = clusterSize1,
    quorum1' = quorum1,
    tracked2' = tracked2,
    tracked3' = tracked3,
    nextIndex2' = nextIndex2,
    nextIndex3' = nextIndex3,
    matchIndex2' = matchIndex2,
    matchIndex3' = matchIndex3,
    heartbeatTargets' = expectedTargets(voter2, voter3, learner3),
  }

  action Noop = all {
    role1' = role1,
    leaderId1' = leaderId1,
    currentTerm1' = currentTerm1,
    commitIndex1' = commitIndex1,
    voter2' = voter2,
    voter3' = voter3,
    learner3' = learner3,
    clusterSize1' = clusterSize1,
    quorum1' = quorum1,
    tracked2' = tracked2,
    tracked3' = tracked3,
    nextIndex2' = nextIndex2,
    nextIndex3' = nextIndex3,
    matchIndex2' = matchIndex2,
    matchIndex3' = matchIndex3,
    heartbeatTargets' = heartbeatTargets,
  }

  action step = any {
    init,
    AddPeer2,
    AddPeer3,
    AddLearner3,
    RemovePeer2,
    RemovePeer3,
    RemoveLearner3,
    SetVoters2,
    SetVoters23,
    SetVoters3,
    TickHeartbeat,
    Noop,
  }

  pure def quorumMatchesVoters =
    quorum1 == expectedQuorum(voter2, voter3)

  pure def clusterSizeMatchesVoters =
    clusterSize1 == expectedClusterSize(voter2, voter3)

  pure def trackedPeersMatchMembership =
    tracked2 == voter2 and tracked3 == (voter3 or learner3)

  pure def untrackedPeersResetProgress =
    (tracked2 or (nextIndex2 == 1 and matchIndex2 == 0)) and
      (tracked3 or (nextIndex3 == 1 and matchIndex3 == 0))

  pure def newPeersStartAtLogEnd =
    (not(tracked2) or nextIndex2 == LAST_LOG_INDEX + 1) and
      (not(tracked3) or nextIndex3 == LAST_LOG_INDEX + 1)

  pure def heartbeatTargetsMatchTrackedPeers =
    heartbeatTargets == List() or heartbeatTargets == expectedTargets(voter2, voter3, learner3)
}
