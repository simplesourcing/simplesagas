package io.simplesource.saga.model.saga;

import io.simplesource.data.Sequence;
import io.simplesource.saga.model.action.SagaAction;
import lombok.Value;

import java.util.*;

@Value
public class Saga<A> {
    public final UUID sagaId;
    public final Map<UUID, SagaAction<A>> actions;
    public final SagaStatus status;
    public final List<SagaError> sagaError;
    public final Sequence sequence;

    public static <A> Saga<A> of(
            UUID sagaId,
            Map<UUID, SagaAction<A>> actions,
            SagaStatus status,
            Sequence sequence) {
        return new Saga<>(sagaId, actions, status, new ArrayList<>(), sequence);
    }

    public Saga<A> updated(SagaStatus status) {
        return updated(status, this.sagaError);
    }

    public Saga<A> updated(SagaStatus status, List<SagaError> sagaError) {
        return updated(this.actions, status, sagaError);
    }

    public Saga<A> updated(Map<UUID, SagaAction<A>> actions, SagaStatus status, List<SagaError> sagaError) {
        return new Saga<>(sagaId, actions, status, sagaError, sequence.next());
    }
}
