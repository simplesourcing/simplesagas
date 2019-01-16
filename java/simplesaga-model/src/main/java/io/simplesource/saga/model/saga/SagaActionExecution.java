package io.simplesource.saga.model.saga;

import io.simplesource.saga.model.action.ActionCommand;
import io.simplesource.saga.model.action.ActionStatus;
import lombok.Value;

import java.util.Optional;
import java.util.UUID;

@Value
public class SagaActionExecution<A> {
    public final UUID actionId;
    public final String actionType;
    public final Optional<ActionCommand<A>> command;
    public final ActionStatus status;
}
