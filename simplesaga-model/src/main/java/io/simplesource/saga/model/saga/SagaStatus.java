package io.simplesource.saga.model.saga;

/**
 * Represents the overall status of the saga.
 *
 * @see io.simplesource.saga.model.action.ActionStatus
 */
public enum SagaStatus {
    /**
     * The saga has not started. This is the initial state.
     */
    NotStarted,
    /**
     * The saga has started processing, and one or more actions have either started processing or completed, but there are still action that have not completed.
     */
    InProgress,
    /**
     * All actions in the saga have completed successfully.
     */
    Completed,
    /**
     * An action has failed, but there are still actions that are in parallel to the failed actions that are in still in progress.
     */
    FailurePending,
    /**
     * An action has failed, and actions are executing in undo mode (i.e. the undo command for the action is being processed).
     */
    InFailure,
    /**
     * One or more actions have failed, and all actions that had previously succeed have been undone, or have been bypassed if no undo is available.
     */
    Failed
}
