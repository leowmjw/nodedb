module ApplyAckIdentity {
  var lastCase: str
  var receiverResolved: bool
  var lastResultKind: str
  var lastPayloadLen: int
  var lastExpectedKey: int
  var lastAppliedKey: int
  var lastStoredBeforeRegister: bool
  var lastRetryAttempts: int
  var lastRoutingLeader: int

  action init = all {
    lastCase' = "None",
    receiverResolved' = false,
    lastResultKind' = "None",
    lastPayloadLen' = 0,
    lastExpectedKey' = 0,
    lastAppliedKey' = 0,
    lastStoredBeforeRegister' = false,
    lastRetryAttempts' = 0,
    lastRoutingLeader' = 0,
  }

  action RegisterThenCompleteMatch = all {
    lastCase' = "RegisterThenCompleteMatch",
    receiverResolved' = true,
    lastResultKind' = "Ok",
    lastPayloadLen' = 6,
    lastExpectedKey' = 43690,
    lastAppliedKey' = 43690,
    lastStoredBeforeRegister' = false,
    lastRetryAttempts' = 0,
    lastRoutingLeader' = 0,
  }

  action RegisterThenCompleteMismatch = all {
    lastCase' = "RegisterThenCompleteMismatch",
    receiverResolved' = true,
    lastResultKind' = "RetryableLeaderChange",
    lastPayloadLen' = 0,
    lastExpectedKey' = 43690,
    lastAppliedKey' = 48059,
    lastStoredBeforeRegister' = false,
    lastRetryAttempts' = 0,
    lastRoutingLeader' = 0,
  }

  action CompleteThenRegisterMatch = all {
    lastCase' = "CompleteThenRegisterMatch",
    receiverResolved' = true,
    lastResultKind' = "Ok",
    lastPayloadLen' = 6,
    lastExpectedKey' = 43690,
    lastAppliedKey' = 43690,
    lastStoredBeforeRegister' = true,
    lastRetryAttempts' = 0,
    lastRoutingLeader' = 0,
  }

  action CompleteThenRegisterMismatch = all {
    lastCase' = "CompleteThenRegisterMismatch",
    receiverResolved' = true,
    lastResultKind' = "RetryableLeaderChange",
    lastPayloadLen' = 0,
    lastExpectedKey' = 43690,
    lastAppliedKey' = 48059,
    lastStoredBeforeRegister' = true,
    lastRetryAttempts' = 0,
    lastRoutingLeader' = 0,
  }

  action CompleteNoopOverwriteThenRegister = all {
    lastCase' = "CompleteNoopOverwriteThenRegister",
    receiverResolved' = true,
    lastResultKind' = "RetryableLeaderChange",
    lastPayloadLen' = 0,
    lastExpectedKey' = 43690,
    lastAppliedKey' = 0,
    lastStoredBeforeRegister' = true,
    lastRetryAttempts' = 0,
    lastRoutingLeader' = 0,
  }

  action RetryLeaderChangeThenSuccess = all {
    lastCase' = "RetryLeaderChangeThenSuccess",
    receiverResolved' = true,
    lastResultKind' = "Ok",
    lastPayloadLen' = 7,
    lastExpectedKey' = 0,
    lastAppliedKey' = 0,
    lastStoredBeforeRegister' = false,
    lastRetryAttempts' = 2,
    lastRoutingLeader' = 0,
  }

  action RetryNotLeaderThenSuccess = all {
    lastCase' = "RetryNotLeaderThenSuccess",
    receiverResolved' = true,
    lastResultKind' = "Ok",
    lastPayloadLen' = 7,
    lastExpectedKey' = 0,
    lastAppliedKey' = 0,
    lastStoredBeforeRegister' = false,
    lastRetryAttempts' = 2,
    lastRoutingLeader' = 9,
  }

  action Noop = all {
    lastCase' = lastCase,
    receiverResolved' = receiverResolved,
    lastResultKind' = lastResultKind,
    lastPayloadLen' = lastPayloadLen,
    lastExpectedKey' = lastExpectedKey,
    lastAppliedKey' = lastAppliedKey,
    lastStoredBeforeRegister' = lastStoredBeforeRegister,
    lastRetryAttempts' = lastRetryAttempts,
    lastRoutingLeader' = lastRoutingLeader,
  }

  action step = any {
    init,
    RegisterThenCompleteMatch,
    RegisterThenCompleteMismatch,
    CompleteThenRegisterMatch,
    CompleteThenRegisterMismatch,
    CompleteNoopOverwriteThenRegister,
    RetryLeaderChangeThenSuccess,
    RetryNotLeaderThenSuccess,
    Noop,
  }

  pure def mismatchMustRetry =
    not((lastCase == "RegisterThenCompleteMismatch") or (lastCase == "CompleteThenRegisterMismatch"))
      or lastResultKind == "RetryableLeaderChange"

  pure def lateMismatchMustRetry =
    lastCase != "CompleteThenRegisterMismatch"
      or (lastStoredBeforeRegister and lastResultKind == "RetryableLeaderChange")

  pure def noopOverwriteMustRetry =
    lastCase != "CompleteNoopOverwriteThenRegister"
      or (lastAppliedKey == 0 and lastResultKind == "RetryableLeaderChange")

  pure def matchingCompletionCanSucceed =
    not((lastCase == "RegisterThenCompleteMatch") or (lastCase == "CompleteThenRegisterMatch"))
      or (receiverResolved and lastResultKind == "Ok" and lastPayloadLen == 6)

  pure def retryableLeaderChangeIsRetried =
    lastCase != "RetryLeaderChangeThenSuccess"
      or (lastRetryAttempts == 2 and lastResultKind == "Ok")

  pure def notLeaderHintUpdatesRouting =
    lastCase != "RetryNotLeaderThenSuccess"
      or (lastRetryAttempts == 2 and lastRoutingLeader == 9 and lastResultKind == "Ok")
}
