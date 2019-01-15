package io.simplesource.saga.model.messages;

import io.simplesource.data.Result;
import io.simplesource.saga.model.saga.SagaError;
import lombok.Value;

import java.util.UUID;

@Value
public class ActionResponse {
    public final UUID sagaId;
    public final UUID actionId;
    public final UUID commandId;
    public final Result<SagaError, Boolean> result;
}
