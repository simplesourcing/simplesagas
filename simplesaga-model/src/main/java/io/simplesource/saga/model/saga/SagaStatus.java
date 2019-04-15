package io.simplesource.saga.model.saga;

/**
 * The enum Saga status.
 */
public enum SagaStatus {
    /**
     * Not started saga status.
     */
    NotStarted,
    /**
     * In progress saga status.
     */
    InProgress,
    /**
     * Completed saga status.
     */
    Completed,
    /**
     * Failure pending saga status.
     */
    FailurePending,
    /**
     * In failure saga status.
     */
    InFailure,
    /**
     * Failed saga status.
     */
    Failed
}
