package io.simplesource.saga.model.saga;

import io.simplesource.data.Sequence;
import lombok.Value;

import java.util.Map;
import java.util.Optional;
import java.util.UUID;

@Value
public class Saga<A> {
    public final UUID sagaId;
    public final Map<UUID, SagaAction<A>> actions;
    public final SagaStatus status;
    public final Optional<SagaError> sagaError;
    public final Sequence sequence;

    public static <A> Saga<A> of(
            UUID sagaId,
            Map<UUID, SagaAction<A>> actions,
            SagaStatus status,
            Sequence sequence) {
        return new Saga<>(sagaId, actions, status, Optional.empty(), sequence);
    }

    public Saga<A> updated(SagaStatus status) {
        return updated(status, this.sagaError);
    }

    public Saga<A> updated(SagaStatus status, Optional<SagaError> sagaError) {
        return updated(this.actions, status, this.sagaError);
    }

    public Saga<A> updated(Map<UUID, SagaAction<A>> actions, SagaStatus status, Optional<SagaError> sagaError) {
        return new Saga<>(sagaId, actions, status, sagaError, sequence.next());
    }
}
