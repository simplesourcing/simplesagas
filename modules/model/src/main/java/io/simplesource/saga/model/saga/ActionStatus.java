package io.simplesource.saga.model.saga;

public enum ActionStatus {
    Pending,
    InProgress,
    Completed,
    Failed,
    InUndo,
    Undone,
    UndoBypassed,
    UndoFailed
}
