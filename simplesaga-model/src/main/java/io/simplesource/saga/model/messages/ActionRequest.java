package io.simplesource.saga.model.messages;

import io.simplesource.saga.model.action.ActionCommand;
import io.simplesource.saga.model.action.ActionId;
import io.simplesource.saga.model.saga.SagaId;
import lombok.Builder;
import lombok.Value;

/**
 * An action request, published by the saga coordinator in the action request topic.
 * <p>
 * The action request topic is the topic for the {@link ActionCommand#actionType} action type of {@code actionCommand} (note that each action type has its own set of request and respense topics).
 * <p>
 * The action processor for this action type consumes this request, starts processing the action, and publishes an {@link ActionResponse} when processing is complete.
 * <p>
 * The action request and response is essentially the interface between the saga coordinator and the action processor. Although the saga coordinator and action processor can be hosted in
 * different processes, they must share the same {@code A} type, and also must have Serdes that are compatible with each other.
 *
 * @param <A> A representation of an action command that is shared across all actions in the saga. This is typically a generic type, such as Json, or if using Avro serialization, SpecificRecord or GenericRecord
 */
@Value(staticConstructor = "of")
public class ActionRequest<A> {
    /**
     * The saga id uniquely identifies the saga, and is used as the key for the action request and responses.
     */
    public final SagaId sagaId;
    /**
     * The action id uniquely identifies an action in the saga dependency graph.
     */
    public final ActionId actionId;
    /**
     * The Action command specifies the details of the command required for processing by the action processor.
     */
    public final ActionCommand<A> actionCommand;
    /**
     * Indicates whether or not this action is processing in undo (compensation) mode.
     */
    public final Boolean isUndo;
}
