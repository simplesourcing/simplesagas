package io.simplesource.saga.model.messages;

import io.simplesource.api.CommandId;
import io.simplesource.data.Result;
import io.simplesource.saga.model.action.ActionId;
import io.simplesource.saga.model.action.UndoCommand;
import io.simplesource.saga.model.saga.SagaError;
import io.simplesource.saga.model.saga.SagaId;
import lombok.Value;

import java.util.Optional;

/**
 * An action response, published by the action processor that receive the corresponding {@link ActionResponse} in the action response topic.
 * <p>
 * The saga coordinator consumes this response and updates the action and saga state as appropriate.
 * <p>
 * The action request and response is essentially the interface between the saga coordinator and the action processor. Although the saga coordinator and action processor can be hosted in
 * different processes, they must share the same {@code A} type, and also must have Serdes that are compatible with each other.
 *
 * @param <A> a representation of an action command that is shared across all actions in the saga. This is typically a generic type, such as Json, or if using Avro serialization, SpecificRecord or GenericRecord
 */

@Value(staticConstructor="of")
public class ActionResponse<A> {
    /**
     * The saga id uniquely identifies the saga, and is used as the key for the action request and responses.
     */
    public final SagaId sagaId;
    /**
     * The action id uniquely identifies an action in the saga dependency graph.
     */
    public final ActionId actionId;
    /**
     * The {@link io.simplesource.saga.model.action.ActionCommand#commandId command id} is the unique identifier for an action command execution.
     */
    public final CommandId commandId;
    /**
     * Indicates whether or not this action is processing in undo (compensation) mode.
     */
    public final Boolean isUndo;
    /**
     * The result, consisting of a {@code SagaError} if processing fail and indicating success if processing succeeds.
     * <p>
     * The action processor can optionally return an undo command for a non-undo action that processes successfully.
     * If set, this replaces any undo command that may already be defined in the dependency graph.
     */
    public final Result<SagaError, Optional<UndoCommand<A>>> result;
}
