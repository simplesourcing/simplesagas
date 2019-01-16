package io.simplesource.saga.model.messages;

import io.simplesource.saga.model.saga.Saga;
import lombok.Value;

import java.util.UUID;

@Value
public class SagaRequest<A> {
    public final UUID sagaId;
    public final Saga<A> initialState;
}
