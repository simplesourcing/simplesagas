package io.simplesource.saga.model.action;

import lombok.Value;

import java.util.UUID;

@Value
public class ActionCommand<A> {
    public final UUID commandId;
    public final A command;
}

