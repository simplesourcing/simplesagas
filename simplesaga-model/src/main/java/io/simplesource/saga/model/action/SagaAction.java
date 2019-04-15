package io.simplesource.saga.model.action;

import io.simplesource.saga.model.saga.SagaError;
import lombok.Value;

import java.util.List;
import java.util.Optional;
import java.util.Set;


/**
 * The representation of an action within a Saga
 *
 * @param <A> the type parameter
 */
@Value(staticConstructor = "of")
public class SagaAction<A> {
    /**
     * The Action id uniquely identifies the action within the Saga dependency graph.
     */
    public final ActionId actionId;
    /**
     * The action command for regular processing of the action.
     */
    public final ActionCommand<A> command;
    /**
     * The command for processing of the action when in undo (compensation) mode.
     * <p>
     * {@code undoCommand} can be defined statically as part of the initial saga definition, and can be (re)defined
     * dynamically by the returning a new undo command from the action processor when processing in regular mode.
     */
    public final Optional<ActionCommand<A>> undoCommand;
    /**
     * The set dependencies of an action (i.e. all the actions that must have completed before processing of this action can commence).
     */
    public final Set<ActionId> dependencies;
    /**
     * The status of the action.
     */
    public final ActionStatus status;
    /**
     * A list of error may have occurred while processing an action. Typically if {@code ActionStatus} is one of the failure status, this list will be populated with the errors that occurred.
     */
    public final List<SagaError> error;
    /**
     * Th number of times that processing of an action has been re-attempted.
     */
    public final int retryCount;

    /**
     * Returns an new {@code SagaAction} with just the status updated
     *
     * @param newStatus the new status
     * @return the saga action
     */
    public SagaAction<A> updated(ActionStatus newStatus) {
        return new SagaAction<>(actionId, command, undoCommand, dependencies, newStatus, error, retryCount);
    }
}
