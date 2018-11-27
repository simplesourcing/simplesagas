package io.simplesource.saga.model.messages;

import io.simplesource.data.Result;
import io.simplesource.data.Sequence;
import io.simplesource.saga.model.saga.SagaError;
import lombok.Value;

import java.util.UUID;

@Value
public class SagaResponse {
    public final UUID sagaId;
    public final Result<SagaError, Sequence> result;
}
