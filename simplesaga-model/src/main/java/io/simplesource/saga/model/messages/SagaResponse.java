package io.simplesource.saga.model.messages;

import io.simplesource.data.Result;
import io.simplesource.data.Sequence;
import io.simplesource.saga.model.saga.SagaError;
import io.simplesource.saga.model.saga.SagaId;
import lombok.Value;

@Value(staticConstructor = "of")
public class SagaResponse {
    public final SagaId sagaId;
    public final Result<SagaError, Sequence> result;
}
