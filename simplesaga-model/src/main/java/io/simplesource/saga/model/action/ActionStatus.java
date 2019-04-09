package io.simplesource.saga.model.action;

public enum ActionStatus {
    Pending,
    InProgress,
    Completed,
    Failed,
    UndoInProgress,
    Undone,
    UndoBypassed,
    UndoFailed,
    RetryAwaiting,
    RetryCompleted
}
