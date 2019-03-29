package io.simplesource.saga.model.messages;

import io.simplesource.saga.model.saga.Saga;
import io.simplesource.saga.model.saga.SagaId;
import lombok.Value;

@Value(staticConstructor = "of")
public class SagaRequest<A> {
    public final SagaId sagaId;
    public final Saga<A> initialState;
}
