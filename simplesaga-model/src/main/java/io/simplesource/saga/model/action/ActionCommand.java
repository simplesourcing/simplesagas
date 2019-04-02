package io.simplesource.saga.model.action;

import io.simplesource.api.CommandId;
import lombok.Value;

@Value(staticConstructor = "of")
public class ActionCommand<A> {
    public final CommandId commandId;
    public final A command;
    public final String actionType;
}

