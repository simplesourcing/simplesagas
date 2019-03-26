package io.simplesource.saga.model.saga;

import io.simplesource.data.Sequence;
import io.simplesource.saga.model.action.ActionId;
import io.simplesource.saga.model.action.SagaAction;
import lombok.Value;

import java.util.*;

@Value
public class Saga<A> {
    public final SagaId sagaId;
    public final Map<ActionId, SagaAction<A>> actions;
    public final SagaStatus status;
    public final List<SagaError> sagaError;
    public final Sequence sequence;

    public static <A> Saga<A> of(
            SagaId sagaId,
            Map<ActionId, SagaAction<A>> actions,
            SagaStatus status,
            Sequence sequence) {
        return new Saga<>(sagaId, actions, status, Collections.emptyList(), sequence);
    }

    public Saga<A> updated(SagaStatus status) {
        return updated(status, this.sagaError);
    }

    public Saga<A> updated(SagaStatus status, List<SagaError> sagaError) {
        return updated(this.actions, status, sagaError);
    }

    public Saga<A> updated(Map<ActionId, SagaAction<A>> actions, SagaStatus status, List<SagaError> sagaError) {
        return new Saga<>(sagaId, actions, status, sagaError, sequence.next());
    }
}
