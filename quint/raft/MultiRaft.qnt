module MultiRaft {
  var mounted0: bool
  var mounted1: bool
  var mounted2: bool
  var groupCount: int
  var routeShard0: int
  var routeShard1: int
  var vshards0: int
  var vshards1: int
  var vshards2: int
  var role0: str
  var role1: str
  var role2: str
  var leader0: int
  var leader1: int
  var leader2: int
  var term0: int
  var term1: int
  var term2: int
  var commit0: int
  var commit1: int
  var commit2: int
  var readyCommit0: int
  var readyCommit1: int
  var readyCommit2: int
  var applied0: int
  var applied1: int
  var applied2: int
  var memberCount0: int
  var memberCount1: int
  var memberCount2: int
  var lastProposalKind: str
  var lastProposalGroup: int
  var lastProposalOk: bool
  var lastProposalNotLeader: bool
  var lastProposalIndex: int
  var lastReadyGroups: List[int]
  var lastReadyCommittedTotal: int

  pure def boolToInt(b) =
    if (b) 1 else 0

  pure def mountedCount(m0, m1, m2) =
    boolToInt(m0) + boolToInt(m1) + boolToInt(m2)

  action init = all {
    mounted0' = false,
    mounted1' = false,
    mounted2' = false,
    groupCount' = 0,
    routeShard0' = 1,
    routeShard1' = 2,
    vshards0' = 0,
    vshards1' = 512,
    vshards2' = 512,
    role0' = "Absent",
    role1' = "Absent",
    role2' = "Absent",
    leader0' = 0,
    leader1' = 0,
    leader2' = 0,
    term0' = 0,
    term1' = 0,
    term2' = 0,
    commit0' = 0,
    commit1' = 0,
    commit2' = 0,
    readyCommit0' = 0,
    readyCommit1' = 0,
    readyCommit2' = 0,
    applied0' = 0,
    applied1' = 0,
    applied2' = 0,
    memberCount0' = 0,
    memberCount1' = 0,
    memberCount2' = 0,
    lastProposalKind' = "None",
    lastProposalGroup' = -1,
    lastProposalOk' = false,
    lastProposalNotLeader' = false,
    lastProposalIndex' = 0,
    lastReadyGroups' = List(),
    lastReadyCommittedTotal' = 0,
  }

  action MountVotingGroups = all {
    not(mounted0),
    not(mounted1),
    mounted0' = true,
    mounted1' = true,
    mounted2' = mounted2,
    groupCount' = mountedCount(true, true, mounted2),
    routeShard0' = routeShard0,
    routeShard1' = routeShard1,
    vshards0' = vshards0,
    vshards1' = vshards1,
    vshards2' = vshards2,
    role0' = "Follower",
    role1' = "Follower",
    role2' = role2,
    leader0' = 0,
    leader1' = 0,
    leader2' = leader2,
    term0' = 0,
    term1' = 0,
    term2' = term2,
    commit0' = 0,
    commit1' = 0,
    commit2' = commit2,
    readyCommit0' = 0,
    readyCommit1' = 0,
    readyCommit2' = readyCommit2,
    applied0' = 0,
    applied1' = 0,
    applied2' = applied2,
    memberCount0' = 1,
    memberCount1' = 1,
    memberCount2' = memberCount2,
    lastProposalKind' = "None",
    lastProposalGroup' = -1,
    lastProposalOk' = false,
    lastProposalNotLeader' = false,
    lastProposalIndex' = 0,
    lastReadyGroups' = List(),
    lastReadyCommittedTotal' = 0,
  }

  action MountLearnerGroup = all {
    not(mounted2),
    mounted0' = mounted0,
    mounted1' = mounted1,
    mounted2' = true,
    groupCount' = mountedCount(mounted0, mounted1, true),
    routeShard0' = routeShard0,
    routeShard1' = routeShard1,
    vshards0' = vshards0,
    vshards1' = vshards1,
    vshards2' = vshards2,
    role0' = role0,
    role1' = role1,
    role2' = "Learner",
    leader0' = leader0,
    leader1' = leader1,
    leader2' = 0,
    term0' = term0,
    term1' = term1,
    term2' = 0,
    commit0' = commit0,
    commit1' = commit1,
    commit2' = 0,
    readyCommit0' = readyCommit0,
    readyCommit1' = readyCommit1,
    readyCommit2' = 0,
    applied0' = applied0,
    applied1' = applied1,
    applied2' = 0,
    memberCount0' = memberCount0,
    memberCount1' = memberCount1,
    memberCount2' = 1,
    lastProposalKind' = "None",
    lastProposalGroup' = -1,
    lastProposalOk' = false,
    lastProposalNotLeader' = false,
    lastProposalIndex' = 0,
    lastReadyGroups' = List(),
    lastReadyCommittedTotal' = 0,
  }

  action TickAll = all {
    mounted0,
    mounted1,
    mounted0' = mounted0,
    mounted1' = mounted1,
    mounted2' = mounted2,
    groupCount' = groupCount,
    routeShard0' = routeShard0,
    routeShard1' = routeShard1,
    vshards0' = vshards0,
    vshards1' = vshards1,
    vshards2' = vshards2,
    role0' = "Leader",
    role1' = "Leader",
    role2' = role2,
    leader0' = 1,
    leader1' = 1,
    leader2' = leader2,
    term0' = if (term0 == 0) 1 else term0,
    term1' = if (term1 == 0) 1 else term1,
    term2' = term2,
    commit0' = if (commit0 == 0) 1 else commit0,
    commit1' = if (commit1 == 0) 1 else commit1,
    commit2' = commit2,
    readyCommit0' = if (term0 == 0) {
      1
    } else if (commit0 > readyCommit0) {
      commit0
    } else {
      readyCommit0
    },
    readyCommit1' = if (term1 == 0) {
      1
    } else if (commit1 > readyCommit1) {
      commit1
    } else {
      readyCommit1
    },
    readyCommit2' = readyCommit2,
    applied0' = applied0,
    applied1' = applied1,
    applied2' = applied2,
    memberCount0' = memberCount0,
    memberCount1' = memberCount1,
    memberCount2' = memberCount2,
    lastProposalKind' = "None",
    lastProposalGroup' = -1,
    lastProposalOk' = false,
    lastProposalNotLeader' = false,
    lastProposalIndex' = 0,
    lastReadyGroups' = if (term0 == 0 or term1 == 0) {
      List(0, 1)
    } else if (commit0 > readyCommit0 and commit1 > readyCommit1) {
      List(0, 1)
    } else if (commit0 > readyCommit0) {
      List(0)
    } else if (commit1 > readyCommit1) {
      List(1)
    } else {
      List()
    },
    lastReadyCommittedTotal' = if (term0 == 0 or term1 == 0) {
      2
    } else {
      (commit0 - readyCommit0) + (commit1 - readyCommit1)
    },
  }

  action AdvanceReady = all {
    mounted0' = mounted0,
    mounted1' = mounted1,
    mounted2' = mounted2,
    groupCount' = groupCount,
    routeShard0' = routeShard0,
    routeShard1' = routeShard1,
    vshards0' = vshards0,
    vshards1' = vshards1,
    vshards2' = vshards2,
    role0' = role0,
    role1' = role1,
    role2' = role2,
    leader0' = leader0,
    leader1' = leader1,
    leader2' = leader2,
    term0' = term0,
    term1' = term1,
    term2' = term2,
    commit0' = commit0,
    commit1' = commit1,
    commit2' = commit2,
    readyCommit0' = readyCommit0,
    readyCommit1' = readyCommit1,
    readyCommit2' = readyCommit2,
    applied0' = if (lastReadyGroups == List(0, 1) or lastReadyGroups == List(0)) commit0 else applied0,
    applied1' = if (lastReadyGroups == List(0, 1) or lastReadyGroups == List(1)) commit1 else applied1,
    applied2' = applied2,
    memberCount0' = memberCount0,
    memberCount1' = memberCount1,
    memberCount2' = memberCount2,
    lastProposalKind' = "None",
    lastProposalGroup' = -1,
    lastProposalOk' = false,
    lastProposalNotLeader' = false,
    lastProposalIndex' = 0,
    lastReadyGroups' = List(),
    lastReadyCommittedTotal' = 0,
  }

  action ProposeVShard0 = all {
    mounted1,
    role1 == "Leader",
    mounted0' = mounted0,
    mounted1' = mounted1,
    mounted2' = mounted2,
    groupCount' = groupCount,
    routeShard0' = routeShard0,
    routeShard1' = routeShard1,
    vshards0' = vshards0,
    vshards1' = vshards1,
    vshards2' = vshards2,
    role0' = role0,
    role1' = role1,
    role2' = role2,
    leader0' = leader0,
    leader1' = leader1,
    leader2' = leader2,
    term0' = term0,
    term1' = term1,
    term2' = term2,
    commit0' = commit0,
    commit1' = commit1 + 1,
    commit2' = commit2,
    readyCommit0' = readyCommit0,
    readyCommit1' = readyCommit1,
    readyCommit2' = readyCommit2,
    applied0' = applied0,
    applied1' = applied1,
    applied2' = applied2,
    memberCount0' = memberCount0,
    memberCount1' = memberCount1,
    memberCount2' = memberCount2,
    lastProposalKind' = "VShard0",
    lastProposalGroup' = 1,
    lastProposalOk' = true,
    lastProposalNotLeader' = false,
    lastProposalIndex' = commit1 + 1,
    lastReadyGroups' = List(),
    lastReadyCommittedTotal' = 0,
  }

  action ProposeVShard1 = all {
    mounted2,
    role2 == "Learner",
    mounted0' = mounted0,
    mounted1' = mounted1,
    mounted2' = mounted2,
    groupCount' = groupCount,
    routeShard0' = routeShard0,
    routeShard1' = routeShard1,
    vshards0' = vshards0,
    vshards1' = vshards1,
    vshards2' = vshards2,
    role0' = role0,
    role1' = role1,
    role2' = role2,
    leader0' = leader0,
    leader1' = leader1,
    leader2' = leader2,
    term0' = term0,
    term1' = term1,
    term2' = term2,
    commit0' = commit0,
    commit1' = commit1,
    commit2' = commit2,
    readyCommit0' = readyCommit0,
    readyCommit1' = readyCommit1,
    readyCommit2' = readyCommit2,
    applied0' = applied0,
    applied1' = applied1,
    applied2' = applied2,
    memberCount0' = memberCount0,
    memberCount1' = memberCount1,
    memberCount2' = memberCount2,
    lastProposalKind' = "VShard1",
    lastProposalGroup' = 2,
    lastProposalOk' = false,
    lastProposalNotLeader' = true,
    lastProposalIndex' = 0,
    lastReadyGroups' = List(),
    lastReadyCommittedTotal' = 0,
  }

  action ProposeMetadata = all {
    mounted0,
    role0 == "Leader",
    mounted0' = mounted0,
    mounted1' = mounted1,
    mounted2' = mounted2,
    groupCount' = groupCount,
    routeShard0' = routeShard0,
    routeShard1' = routeShard1,
    vshards0' = vshards0,
    vshards1' = vshards1,
    vshards2' = vshards2,
    role0' = role0,
    role1' = role1,
    role2' = role2,
    leader0' = leader0,
    leader1' = leader1,
    leader2' = leader2,
    term0' = term0,
    term1' = term1,
    term2' = term2,
    commit0' = commit0 + 1,
    commit1' = commit1,
    commit2' = commit2,
    readyCommit0' = readyCommit0,
    readyCommit1' = readyCommit1,
    readyCommit2' = readyCommit2,
    applied0' = applied0,
    applied1' = applied1,
    applied2' = applied2,
    memberCount0' = memberCount0,
    memberCount1' = memberCount1,
    memberCount2' = memberCount2,
    lastProposalKind' = "Metadata",
    lastProposalGroup' = 0,
    lastProposalOk' = true,
    lastProposalNotLeader' = false,
    lastProposalIndex' = commit0 + 1,
    lastReadyGroups' = List(),
    lastReadyCommittedTotal' = 0,
  }

  action Noop = all {
    mounted0' = mounted0,
    mounted1' = mounted1,
    mounted2' = mounted2,
    groupCount' = groupCount,
    routeShard0' = routeShard0,
    routeShard1' = routeShard1,
    vshards0' = vshards0,
    vshards1' = vshards1,
    vshards2' = vshards2,
    role0' = role0,
    role1' = role1,
    role2' = role2,
    leader0' = leader0,
    leader1' = leader1,
    leader2' = leader2,
    term0' = term0,
    term1' = term1,
    term2' = term2,
    commit0' = commit0,
    commit1' = commit1,
    commit2' = commit2,
    readyCommit0' = readyCommit0,
    readyCommit1' = readyCommit1,
    readyCommit2' = readyCommit2,
    applied0' = applied0,
    applied1' = applied1,
    applied2' = applied2,
    memberCount0' = memberCount0,
    memberCount1' = memberCount1,
    memberCount2' = memberCount2,
    lastProposalKind' = lastProposalKind,
    lastProposalGroup' = lastProposalGroup,
    lastProposalOk' = lastProposalOk,
    lastProposalNotLeader' = lastProposalNotLeader,
    lastProposalIndex' = lastProposalIndex,
    lastReadyGroups' = lastReadyGroups,
    lastReadyCommittedTotal' = lastReadyCommittedTotal,
  }

  action step = any {
    init,
    MountVotingGroups,
    MountLearnerGroup,
    TickAll,
    AdvanceReady,
    ProposeVShard0,
    ProposeVShard1,
    ProposeMetadata,
    Noop,
  }

  pure def groupCountMatchesMounted =
    groupCount == mountedCount(mounted0, mounted1, mounted2)

  pure def routingLayoutStable =
    routeShard0 == 1 and routeShard1 == 2 and vshards0 == 0 and vshards1 == 512 and vshards2 == 512

  pure def mountedStatusesCoherent =
    (not(mounted0) or memberCount0 == 1) and
      (not(mounted1) or memberCount1 == 1) and
      (not(mounted2) or memberCount2 == 1)

  pure def learnerGroupDoesNotLead =
    not(mounted2) or role2 != "Leader"

  pure def proposalOutcomeMatchesRoute =
    (lastProposalKind == "None") or
      (lastProposalKind == "VShard0" and lastProposalGroup == 1 and lastProposalOk and not(lastProposalNotLeader)) or
      (lastProposalKind == "VShard1" and lastProposalGroup == 2 and not(lastProposalOk) and lastProposalNotLeader) or
      (lastProposalKind == "Metadata" and lastProposalGroup == 0 and lastProposalOk and not(lastProposalNotLeader))

  pure def readyGroupsAreMounted =
    lastReadyGroups == List() or lastReadyGroups == List(0) or lastReadyGroups == List(1) or lastReadyGroups == List(0, 1)
}
