package io.simplesource.saga.model.messages;

import io.simplesource.saga.model.saga.*;
import lombok.Value;

import java.util.UUID;

@Value
public class ActionRequest<A> {
    public final UUID sagaId;
    public final UUID actionId;
    public final ActionCommand<A> actionCommand;
    public final String actionType;
}

