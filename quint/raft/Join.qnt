module Join {
  var groupCount: int
  var group0Leader: int
  var topologyVersion: int
  var topologyNodes: int
  var topologyHas2: bool
  var topologyHas7: bool
  var learner2In0: bool
  var learner2In1: bool
  var learner2In2: bool
  var member2In0: bool
  var member2In1: bool
  var member2In2: bool
  var lastJoinKind: str
  var lastJoinMutated: bool
  var lastResponseSuccess: bool
  var lastResponseErrorKind: str
  var lastResponseNodeCount: int
  var lastResponseGroupCount: int
  var lastResponseLearnerAllGroups: bool

  action init = all {
    groupCount' = 3,
    group0Leader' = 1,
    topologyVersion' = 1,
    topologyNodes' = 1,
    topologyHas2' = false,
    topologyHas7' = false,
    learner2In0' = false,
    learner2In1' = false,
    learner2In2' = false,
    member2In0' = false,
    member2In1' = false,
    member2In2' = false,
    lastJoinKind' = "None",
    lastJoinMutated' = false,
    lastResponseSuccess' = false,
    lastResponseErrorKind' = "None",
    lastResponseNodeCount' = 0,
    lastResponseGroupCount' = 0,
    lastResponseLearnerAllGroups' = false,
  }

  action SetRemoteLeaderKnown = all {
    group0Leader == 1,
    groupCount' = groupCount,
    group0Leader' = 7,
    topologyVersion' = topologyVersion + 1,
    topologyNodes' = if (topologyHas7) topologyNodes else topologyNodes + 1,
    topologyHas2' = topologyHas2,
    topologyHas7' = true,
    learner2In0' = learner2In0,
    learner2In1' = learner2In1,
    learner2In2' = learner2In2,
    member2In0' = member2In0,
    member2In1' = member2In1,
    member2In2' = member2In2,
    lastJoinKind' = "None",
    lastJoinMutated' = false,
    lastResponseSuccess' = false,
    lastResponseErrorKind' = "None",
    lastResponseNodeCount' = 0,
    lastResponseGroupCount' = 0,
    lastResponseLearnerAllGroups' = false,
  }

  action HandleJoinRedirect = all {
    group0Leader == 7,
    groupCount' = groupCount,
    group0Leader' = group0Leader,
    topologyVersion' = topologyVersion,
    topologyNodes' = topologyNodes,
    topologyHas2' = topologyHas2,
    topologyHas7' = topologyHas7,
    learner2In0' = learner2In0,
    learner2In1' = learner2In1,
    learner2In2' = learner2In2,
    member2In0' = member2In0,
    member2In1' = member2In1,
    member2In2' = member2In2,
    lastJoinKind' = "Redirect",
    lastJoinMutated' = false,
    lastResponseSuccess' = false,
    lastResponseErrorKind' = "Redirect",
    lastResponseNodeCount' = 0,
    lastResponseGroupCount' = 0,
    lastResponseLearnerAllGroups' = false,
  }

  action HandleJoinNew = all {
    group0Leader == 1,
    not(topologyHas2),
    groupCount' = groupCount,
    group0Leader' = group0Leader,
    topologyVersion' = topologyVersion + 1,
    topologyNodes' = topologyNodes + 1,
    topologyHas2' = true,
    topologyHas7' = topologyHas7,
    learner2In0' = true,
    learner2In1' = true,
    learner2In2' = true,
    member2In0' = false,
    member2In1' = false,
    member2In2' = false,
    lastJoinKind' = "New",
    lastJoinMutated' = true,
    lastResponseSuccess' = true,
    lastResponseErrorKind' = "None",
    lastResponseNodeCount' = topologyNodes + 1,
    lastResponseGroupCount' = 3,
    lastResponseLearnerAllGroups' = true,
  }

  action HandleJoinIdempotent = all {
    group0Leader == 1,
    topologyHas2,
    groupCount' = groupCount,
    group0Leader' = group0Leader,
    topologyVersion' = topologyVersion,
    topologyNodes' = topologyNodes,
    topologyHas2' = topologyHas2,
    topologyHas7' = topologyHas7,
    learner2In0' = learner2In0,
    learner2In1' = learner2In1,
    learner2In2' = learner2In2,
    member2In0' = member2In0,
    member2In1' = member2In1,
    member2In2' = member2In2,
    lastJoinKind' = "Idempotent",
    lastJoinMutated' = false,
    lastResponseSuccess' = true,
    lastResponseErrorKind' = "None",
    lastResponseNodeCount' = topologyNodes,
    lastResponseGroupCount' = 3,
    lastResponseLearnerAllGroups' = learner2In0 and learner2In1 and learner2In2,
  }

  action HandleJoinConflict = all {
    group0Leader == 1,
    topologyHas2,
    groupCount' = groupCount,
    group0Leader' = group0Leader,
    topologyVersion' = topologyVersion,
    topologyNodes' = topologyNodes,
    topologyHas2' = topologyHas2,
    topologyHas7' = topologyHas7,
    learner2In0' = learner2In0,
    learner2In1' = learner2In1,
    learner2In2' = learner2In2,
    member2In0' = member2In0,
    member2In1' = member2In1,
    member2In2' = member2In2,
    lastJoinKind' = "Conflict",
    lastJoinMutated' = false,
    lastResponseSuccess' = false,
    lastResponseErrorKind' = "Conflict",
    lastResponseNodeCount' = 0,
    lastResponseGroupCount' = 0,
    lastResponseLearnerAllGroups' = false,
  }

  action Noop = all {
    groupCount' = groupCount,
    group0Leader' = group0Leader,
    topologyVersion' = topologyVersion,
    topologyNodes' = topologyNodes,
    topologyHas2' = topologyHas2,
    topologyHas7' = topologyHas7,
    learner2In0' = learner2In0,
    learner2In1' = learner2In1,
    learner2In2' = learner2In2,
    member2In0' = member2In0,
    member2In1' = member2In1,
    member2In2' = member2In2,
    lastJoinKind' = lastJoinKind,
    lastJoinMutated' = lastJoinMutated,
    lastResponseSuccess' = lastResponseSuccess,
    lastResponseErrorKind' = lastResponseErrorKind,
    lastResponseNodeCount' = lastResponseNodeCount,
    lastResponseGroupCount' = lastResponseGroupCount,
    lastResponseLearnerAllGroups' = lastResponseLearnerAllGroups,
  }

  action step = any {
    init,
    SetRemoteLeaderKnown,
    HandleJoinRedirect,
    HandleJoinNew,
    HandleJoinIdempotent,
    HandleJoinConflict,
    Noop,
  }

  pure def successfulJoinMeansLearnerEverywhere =
    not(lastResponseSuccess) or lastResponseLearnerAllGroups

  pure def joinNeverPromotesDirectly =
    not(member2In0) and not(member2In1) and not(member2In2)

  pure def redirectDoesNotMutate =
    lastJoinKind != "Redirect" or not(lastJoinMutated)

  pure def idempotentDoesNotMutate =
    lastJoinKind != "Idempotent" or not(lastJoinMutated)

  pure def conflictDoesNotMutate =
    lastJoinKind != "Conflict" or not(lastJoinMutated)

  pure def successResponseCarriesAllGroups =
    not(lastResponseSuccess) or lastResponseGroupCount == groupCount
}
