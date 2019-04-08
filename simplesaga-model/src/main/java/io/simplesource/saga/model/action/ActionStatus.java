package io.simplesource.saga.model.action;

public enum ActionStatus {
    Pending,
    InProgress,
    Completed,
    Failed,
    InUndo,
    Undone,
    UndoBypassed,
    UndoFailed,
    RetryAwaiting,
    RetryCompleted
}
