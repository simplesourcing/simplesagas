package io.simplesource.saga.model.action;

import lombok.Value;

/**
 * Represents an undo action returned in the {@link io.simplesource.saga.model.messages.ActionResponse action response} to dynamically set the undo (compensation) command for the action for if the saga later fails.
 *
 * @param <A> a representation of an action command that is shared across all actions in the saga. This is typically a generic type, such as Json, or if using Avro serialization, SpecificRecord or GenericRecord
 */
@Value(staticConstructor="of")
public class UndoCommand<A> {
    /**
     * An action command, expressed in a generic type {code A}, that the action processor is able to decode.
     */
    public final A command;
    /**
     * The Action type for the undo command. This determines which action processor is used to process it.
     */
    public final String actionType;
}
