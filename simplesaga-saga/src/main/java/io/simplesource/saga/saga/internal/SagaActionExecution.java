package io.simplesource.saga.saga.internal;

import io.simplesource.saga.model.action.ActionCommand;
import io.simplesource.saga.model.action.ActionId;
import io.simplesource.saga.model.action.ActionStatus;
import lombok.Value;

import java.util.Optional;

@Value(staticConstructor = "of")
class SagaActionExecution<A> {
    public final ActionId actionId;
    public final Optional<ActionCommand<A>> command;
    public final ActionStatus status;
    public final Boolean isUndo;
}
