module Snapshots {
  type LogEntry = { term: int, index: int, data: int }
  type InstallSnapshot = {
    term: int,
    leaderId: int,
    lastIncludedIndex: int,
    lastIncludedTerm: int,
    offset: int,
    data: int,
    done: bool,
    groupId: int,
  }
  type InstallSnapshotEnvelope = { src: int, dst: int, req: InstallSnapshot }

  pure val GROUP_ID = 1
  pure val LEADER = 1
  pure val FOLLOWER = 2

  var role1: str
  var role2: str
  var currentTerm1: int
  var currentTerm2: int
  var leaderId1: int
  var leaderId2: int
  var commitIndex1: int
  var commitIndex2: int
  var lastApplied1: int
  var lastApplied2: int
  var snapshotIndex1: int
  var snapshotIndex2: int
  var snapshotTerm1: int
  var snapshotTerm2: int
  var log1: List[LogEntry]
  var log2: List[LogEntry]
  var nextIndex2: int
  var matchIndex2: int
  var snapshotsNeeded: List[int]
  var snapshotRequests: List[InstallSnapshotEnvelope]

  pure def entry3 = { term: 2, index: 3, data: 9 }

  pure def lastIndex(snapshotIndex, entries) =
    if (entries.length() == 0) snapshotIndex else entries[entries.length() - 1].index

  pure def termAt(snapshotIndex, snapshotTerm, entries, index) =
    if (index == 0) {
      0
    } else if (index == snapshotIndex) {
      snapshotTerm
    } else if (index < snapshotIndex) {
      -1
    } else if (index - snapshotIndex <= entries.length()) {
      entries[index - snapshotIndex - 1].term
    } else {
      -1
    }

  pure def entriesFrom(entries, fromIndex) =
    if (entries.length() == 0 or fromIndex > entries[entries.length() - 1].index) {
      List()
    } else {
      entries.slice(fromIndex - entries[0].index, entries.length())
    }

  def installSnapshotReq(dst) = {
    src: LEADER,
    dst: dst,
    req: {
      term: currentTerm1,
      leaderId: LEADER,
      lastIncludedIndex: snapshotIndex1,
      lastIncludedTerm: snapshotTerm1,
      offset: 0,
      data: 0,
      done: true,
      groupId: GROUP_ID,
    },
  }

  action init = all {
    role1' = "Leader",
    role2' = "Follower",
    currentTerm1' = 2,
    currentTerm2' = 1,
    leaderId1' = 1,
    leaderId2' = 0,
    commitIndex1' = 3,
    commitIndex2' = 0,
    lastApplied1' = 0,
    lastApplied2' = 0,
    snapshotIndex1' = 2,
    snapshotIndex2' = 0,
    snapshotTerm1' = 1,
    snapshotTerm2' = 0,
    log1' = List(entry3),
    log2' = List(),
    nextIndex2' = 1,
    matchIndex2' = 0,
    snapshotsNeeded' = List(),
    snapshotRequests' = List(),
  }

  action RequestSnapshotNeeded = all {
    role1 == "Leader",
    nextIndex2 <= snapshotIndex1,
    snapshotsNeeded.length() == 0,
    role1' = role1,
    role2' = role2,
    currentTerm1' = currentTerm1,
    currentTerm2' = currentTerm2,
    leaderId1' = leaderId1,
    leaderId2' = leaderId2,
    commitIndex1' = commitIndex1,
    commitIndex2' = commitIndex2,
    lastApplied1' = lastApplied1,
    lastApplied2' = lastApplied2,
    snapshotIndex1' = snapshotIndex1,
    snapshotIndex2' = snapshotIndex2,
    snapshotTerm1' = snapshotTerm1,
    snapshotTerm2' = snapshotTerm2,
    log1' = log1,
    log2' = log2,
    nextIndex2' = nextIndex2,
    matchIndex2' = matchIndex2,
    snapshotsNeeded' = List(FOLLOWER),
    snapshotRequests' = snapshotRequests,
  }

  action SendInstallSnapshot = all {
    snapshotsNeeded.length() > 0,
    snapshotsNeeded[0] == FOLLOWER,
    role1' = role1,
    role2' = role2,
    currentTerm1' = currentTerm1,
    currentTerm2' = currentTerm2,
    leaderId1' = leaderId1,
    leaderId2' = leaderId2,
    commitIndex1' = commitIndex1,
    commitIndex2' = commitIndex2,
    lastApplied1' = lastApplied1,
    lastApplied2' = lastApplied2,
    snapshotIndex1' = snapshotIndex1,
    snapshotIndex2' = snapshotIndex2,
    snapshotTerm1' = snapshotTerm1,
    snapshotTerm2' = snapshotTerm2,
    log1' = log1,
    log2' = log2,
    nextIndex2' = nextIndex2,
    matchIndex2' = matchIndex2,
    snapshotsNeeded' = snapshotsNeeded.slice(1, snapshotsNeeded.length()),
    snapshotRequests' = snapshotRequests.append(installSnapshotReq(FOLLOWER)),
  }

  action HandleInstallSnapshot = {
    val msg = snapshotRequests[0]
    val higherTerm = msg.req.term > currentTerm2
    val apply = msg.req.done and msg.req.lastIncludedIndex > snapshotIndex2
    val newCommit = if (commitIndex2 < msg.req.lastIncludedIndex) msg.req.lastIncludedIndex else commitIndex2
    val newApplied = if (lastApplied2 < msg.req.lastIncludedIndex) msg.req.lastIncludedIndex else lastApplied2
    all {
      snapshotRequests.length() > 0,
      role1' = role1,
      role2' = if (higherTerm) "Follower" else role2,
      currentTerm1' = currentTerm1,
      currentTerm2' = if (higherTerm) msg.req.term else currentTerm2,
      leaderId1' = leaderId1,
      leaderId2' = msg.req.leaderId,
      commitIndex1' = commitIndex1,
      commitIndex2' = if (apply) newCommit else commitIndex2,
      lastApplied1' = lastApplied1,
      lastApplied2' = if (apply) newApplied else lastApplied2,
      snapshotIndex1' = snapshotIndex1,
      snapshotIndex2' = if (apply) msg.req.lastIncludedIndex else snapshotIndex2,
      snapshotTerm1' = snapshotTerm1,
      snapshotTerm2' = if (apply) msg.req.lastIncludedTerm else snapshotTerm2,
      log1' = log1,
      log2' = if (apply and log2.length() > 0 and msg.req.lastIncludedIndex < lastIndex(snapshotIndex2, log2)) {
        entriesFrom(log2, msg.req.lastIncludedIndex + 1)
      } else if (apply) {
        List()
      } else {
        log2
      },
      nextIndex2' = nextIndex2,
      matchIndex2' = matchIndex2,
      snapshotsNeeded' = snapshotsNeeded,
      snapshotRequests' = snapshotRequests.slice(1, snapshotRequests.length()),
    }
  }

  action Noop = all {
    role1' = role1,
    role2' = role2,
    currentTerm1' = currentTerm1,
    currentTerm2' = currentTerm2,
    leaderId1' = leaderId1,
    leaderId2' = leaderId2,
    commitIndex1' = commitIndex1,
    commitIndex2' = commitIndex2,
    lastApplied1' = lastApplied1,
    lastApplied2' = lastApplied2,
    snapshotIndex1' = snapshotIndex1,
    snapshotIndex2' = snapshotIndex2,
    snapshotTerm1' = snapshotTerm1,
    snapshotTerm2' = snapshotTerm2,
    log1' = log1,
    log2' = log2,
    nextIndex2' = nextIndex2,
    matchIndex2' = matchIndex2,
    snapshotsNeeded' = snapshotsNeeded,
    snapshotRequests' = snapshotRequests,
  }

  val snapshotNeededForLaggingPeer = snapshotsNeeded.indices().forall(i => snapshotsNeeded[i] == FOLLOWER and nextIndex2 <= snapshotIndex1)

  val leaderSnapshotBoundaryValid = snapshotIndex1 <= lastIndex(snapshotIndex1, log1) and termAt(snapshotIndex1, snapshotTerm1, log1, snapshotIndex1) == snapshotTerm1

  val followerSnapshotBoundaryValid = snapshotIndex2 <= lastIndex(snapshotIndex2, log2) and termAt(snapshotIndex2, snapshotTerm2, log2, snapshotIndex2) == snapshotTerm2

  val followerCommitAppliedFollowSnapshot = commitIndex2 >= snapshotIndex2 and lastApplied2 >= snapshotIndex2 and lastApplied2 <= commitIndex2

  val postSnapshotLogAfterBoundary = log2.indices().forall(i => log2[i].index > snapshotIndex2)

  action step = any {
    RequestSnapshotNeeded,
    SendInstallSnapshot,
    HandleInstallSnapshot,
    Noop,
  }
}
