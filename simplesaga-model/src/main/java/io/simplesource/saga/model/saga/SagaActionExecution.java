package io.simplesource.saga.model.saga;

import io.simplesource.saga.model.action.ActionCommand;
import io.simplesource.saga.model.action.ActionId;
import io.simplesource.saga.model.action.ActionStatus;
import lombok.Value;

import java.util.Optional;

/**
 * The type Saga action execution.
 *
 * @param <A> the type parameter
 */
@Value(staticConstructor = "of")
public class SagaActionExecution<A> {
    /**
     * The Action id.
     */
    public final ActionId actionId;
    /**
     * The Command.
     */
    public final Optional<ActionCommand<A>> command;
    /**
     * The Status.
     */
    public final ActionStatus status;
    /**
     * The Is undo.
     */
    public final Boolean isUndo;
}
