package io.simplesource.saga.model.messages;

import io.simplesource.saga.model.action.ActionId;
import io.simplesource.saga.model.action.ActionStatus;
import io.simplesource.saga.model.saga.Saga;
import io.simplesource.saga.model.saga.SagaError;
import io.simplesource.saga.model.saga.SagaId;
import io.simplesource.saga.model.saga.SagaStatus;
import lombok.Value;

import java.util.List;
import java.util.Optional;
import java.util.function.Function;

public interface SagaStateTransition {

    @Value(staticConstructor = "of")
    final class SetInitialState<A> implements SagaStateTransition {
        public final Saga<A> sagaState;

        @Override
        public <A> A cata(Function<SetInitialState<?>, A> f1, Function<SagaActionStateChanged<?>, A> f2, Function<SagaStatusChanged, A> f3, Function<TransitionList, A> f4) {
            return f1.apply(this);
        }
    }

    @Value(staticConstructor = "of")
    final class SagaActionStateChanged<A> implements SagaStateTransition {
        public final SagaId sagaId;
        public final ActionId actionId;
        public final ActionStatus actionStatus;
        public final List<SagaError> actionErrors;
        public final Optional<UndoCommand<A>> undoCommand;

        @Override
        public <B> B cata(Function<SetInitialState<?>, B> f1, Function<SagaActionStateChanged<?>, B> f2, Function<SagaStatusChanged, B> f3, Function<TransitionList, B> f4) {
            return f2.apply(this);
        }
    }

    @Value(staticConstructor = "of")
    final class SagaStatusChanged implements SagaStateTransition {
        public final SagaId sagaId;
        public final SagaStatus sagaStatus;
        public final List<SagaError> sagaErrors;


        @Override
        public <B> B cata(Function<SetInitialState<?>, B> f1, Function<SagaActionStateChanged<?>, B> f2, Function<SagaStatusChanged, B> f3, Function<TransitionList, B> f4) {
            return f3.apply(this);
        }
    }

    @Value(staticConstructor = "of")
    final class TransitionList implements SagaStateTransition {
        public final List<SagaActionStateChanged> actions;

        @Override
        public <B> B cata(Function<SetInitialState<?>, B> f1, Function<SagaActionStateChanged<?>, B> f2, Function<SagaStatusChanged, B> f3, Function<TransitionList, B> f4) {
            return f4.apply(this);
        }
    }

    /**
     * Catamorphism over SagaStateTransition
     */
    <B> B cata(
            Function<SetInitialState<?>, B> f1,
            Function<SagaActionStateChanged<?>, B> f2,
            Function<SagaStatusChanged, B> f3,
            Function<TransitionList, B> f4
            );
}
