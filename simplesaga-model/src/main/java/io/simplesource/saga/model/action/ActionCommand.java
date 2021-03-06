package io.simplesource.saga.model.action;

import io.simplesource.api.CommandId;
import lombok.Value;

/**
 * An {@code ActionCommand} is a data class with the information needed:
 * <ul>
 *     <li>to uniquely identify the execution of the action and know whether it has been processed or not.</li>
 *     <li>to determine which action processor to use to process an action.</li>
 *     <li>by the action processor to process the action.</li>
 * </ul>
 *
 * @param <A> a representation of an action command that is shared across all actions in the saga. This is typically a generic type, such as Json, or if using Avro serialization, SpecificRecord or GenericRecord
 */
@Value(staticConstructor = "of")
public class ActionCommand<A> {
    /**
     * The command Id is the unique identifier for an action command. Actions should be idempotent with respect to command Id.
     * <p>
     * This means that:
     * <ul>
     *     <li>
     *         if the action processor receives an action request with a given command Id that it has already seen and processed before, it should
     *         republish the action response and do no further processing.
     *     </li>
     *     <li>
     *         if the saga coordinator retries a failed action, it will republish the action request with a different command Id.
     *     </li>
     *     <li>
     *         the undo action has a distinct command Id from the original action.
     *     </li>
     * </ul>
     */
    public final CommandId commandId;
    /**
     * An action command, expressed in a generic type {@code A}, that the action processor is able to decode.
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
     * @param <A> a representation of an action command that is shared across all actions in the saga. This is typically a generic type, such as Json, or if using Avro serialization, SpecificRecord or GenericRecord
     * @param command    the action command, expressed in a generic type {@code A}, that the action processor is able to decode
     * @param actionType the action type, which determines which action processor is used to process the action
     * @return the action command
     */
    public static <A> ActionCommand<A> of(A command, String actionType) {
        return new ActionCommand<>(CommandId.random(), command, actionType);
    }
}

