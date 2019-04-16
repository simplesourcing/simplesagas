package io.simplesource.saga.model.action;

/**
 * An enum representing the current state of an action within a saga
 *
 * @see io.simplesource.saga.model.saga.SagaStatus
 */
public enum ActionStatus {
    /**
     * The initial state of action, before execution of the action has started.
     */
    Pending,
    /**
     * An action request has been published to start action, but no response has been received.
     */
    InProgress,
    /**
     * Action has completed successfully, and an action response has been received.
     */
    Completed,
    /**
     * Action has failed to complete, possibly after one or more retries.
     */
    Failed,
    /**
     * One of the other saga actions has failed, and a request has been published to undo this action, but no response has been received.
     */
    UndoInProgress,
    /**
     * Undo command for this action has completed successfully.
     */
    Undone,
    /**
     * Action does not have an undo command, so when processing backwards in undo (compensation) mode, processing for this action is bypassed.
     */
    UndoBypassed,
    /**
     * Undo command for this action has failed to complete, possibly after one or more retries.
     */
    UndoFailed,
    /**
     * Action has failed to complete, and a retry has been scheduled to attempt processing again after a certain delay.
     */
    RetryAwaiting,
    /**
     * A transient state which indicates that the waiting period of a retry has completed, and reprocessing can commence.
     */
    RetryCompleted
}
