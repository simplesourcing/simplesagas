package io.simplesource.saga.model.messages;

import io.simplesource.api.CommandId;
import io.simplesource.data.Result;
import io.simplesource.saga.model.action.ActionId;
import io.simplesource.saga.model.saga.SagaError;
import io.simplesource.saga.model.saga.SagaId;
import lombok.Value;

@Value
public class ActionResponse {
    public final SagaId sagaId;
    public final ActionId actionId;
    public final CommandId commandId;
    public final Result<SagaError, Boolean> result;
}
