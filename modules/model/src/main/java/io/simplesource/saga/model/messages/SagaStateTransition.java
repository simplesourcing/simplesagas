package io.simplesource.saga.model.messages;

import io.simplesource.data.NonEmptyList;
import io.simplesource.saga.model.saga.ActionStatus;
import io.simplesource.saga.model.saga.Saga;
import io.simplesource.saga.model.saga.SagaError;
import io.simplesource.saga.model.saga.SagaStatus;
import lombok.Value;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

public interface SagaStateTransition {
    @Value
    class SetInitialState<A> implements SagaStateTransition {
        public final Saga<A> sagaState;
    }

    @Value
    class SagaActionStatusChanged implements SagaStateTransition {
        public final UUID sagaId;
        public final UUID actionId;
        public final ActionStatus actionStatus;
        public final Optional<SagaError> actionError;
    }

    @Value
    class SagaStatusChanged implements SagaStateTransition {
        public final UUID sagaId;
        public final SagaStatus sagaStatus;
        public final Optional<NonEmptyList<SagaError>> actionErrors;
    }

    @Value
    class TransitionList implements SagaStateTransition {
        public final List<SagaActionStatusChanged> actions;
    }
}
