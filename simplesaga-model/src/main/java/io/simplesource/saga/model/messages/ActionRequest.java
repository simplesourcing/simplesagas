package io.simplesource.saga.model.messages;

import io.simplesource.saga.model.action.ActionCommand;
import io.simplesource.saga.model.action.ActionId;
import io.simplesource.saga.model.saga.SagaId;
import lombok.Builder;
import lombok.Value;


@Value
@Builder
public class ActionRequest<A> {
    public final SagaId sagaId;
    public final ActionId actionId;
    public final ActionCommand<A> actionCommand;
    public final String actionType;
}

