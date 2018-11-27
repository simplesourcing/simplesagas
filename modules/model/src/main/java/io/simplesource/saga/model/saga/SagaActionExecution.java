package io.simplesource.saga.model.saga;

import lombok.Value;

import java.util.UUID;

@Value
public class SagaActionExecution<A> {
    public final UUID actionId;
    public final String actionType;
    public final ActionCommand<A> command;
    public final ActionStatus status;
}
