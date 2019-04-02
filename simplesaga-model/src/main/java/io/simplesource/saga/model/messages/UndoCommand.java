package io.simplesource.saga.model.messages;

import lombok.Value;

@Value(staticConstructor="of")
public class UndoCommand<A> {
    public final A command;
    public final String actionType;
}
