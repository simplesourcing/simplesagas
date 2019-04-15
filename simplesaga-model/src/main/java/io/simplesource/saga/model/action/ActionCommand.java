package io.simplesource.saga.model.action;

import io.simplesource.api.CommandId;
import lombok.Value;

/**
 * An {@code ActionCommand} consists of command that the action processor will understand as well as the action type and the command Id
 *
 * @param <A> A representation of an action command that is shared across all actions in the saga. This is typically a generic type, such as Json, or if using Avro serialization, SpecificRecord or GenericRecord
 */
@Value(staticConstructor = "of")
public class ActionCommand<A> {
    /**
     * The Command is the unique identifier for an action command. Actions should be idempotent with respect to command Id.
     * <p>
     * This means that:
     * <ul>
     *     <li>
     *         if the action processor receives an action request with a given command Id that it has already seen and processed before, it should
     *         republish the action response and do no further processing.
     *     </li>
     *     <li>
     *         if the saga coordinator wants to retry a failed action, it will republish the action request with a different command Id.
     *     </li>
     *     <li>
     *         The undo action has a distinct command Id from the original action.
     *     </li>
     * </ul>
     */
    public final CommandId commandId;
    /**
     * An action command, expressed in a generic type {code A}, that the action processor is able to decode.
     */
    public final A command;
    /**
     * Every action processor has an associated action type. This action type identifies which action processor to use to process an action.
     * It also specifies which topics to use for action requests and responses.
     */
    public final String actionType;

    /**
     * Static constructor for action commands
     *
     * @param <A> A representation of an action command that is shared across all actions in the saga. This is typically a generic type, such as Json, or if using Avro serialization, SpecificRecord or GenericRecord
     * @param command    the command
     * @param actionType the action type
     * @return the action command
     */
    public static <A> ActionCommand<A> of(A command, String actionType) {
        return new ActionCommand<>(CommandId.random(), command, actionType);
    }
}

