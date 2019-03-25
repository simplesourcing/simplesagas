package io.simplesource.saga.model.saga;

public enum SagaStatus {
    NotStarted,
    InProgress,
    Completed,
    FailurePending,
    InFailure,
    Failed
}
