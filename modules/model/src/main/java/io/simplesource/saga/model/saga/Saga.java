package io.simplesource.saga.model.saga;

import io.simplesource.data.Sequence;
import lombok.Value;

import java.util.Map;
import java.util.UUID;

@Value
public class Saga<A> {
    public final UUID sagaId;
    public final Map<UUID, SagaAction<A>> actions;
    public final SagaStatus status;
    public final Sequence sequence;
}
