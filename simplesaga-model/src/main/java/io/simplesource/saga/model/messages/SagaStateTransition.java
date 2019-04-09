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

public interface SagaStateTransition<A> {

    @Value(staticConstructor = "of")
    final class SetInitialState<A> implements SagaStateTransition<A> {
        public final Saga<A> sagaState;

        @Override
        public <B> B cata(Function<SetInitialState<A>, B> f1, Function<SagaActionStateChanged<A>, B> f2, Function<SagaStatusChanged<A>, B> f3, Function<TransitionList<A>, B> f4) {
            return f1.apply(this);
        }
    }

    @Value(staticConstructor = "of")
    final class SagaActionStateChanged<A> implements SagaStateTransition<A> {
        public final SagaId sagaId;
        public final ActionId actionId;
        public final ActionStatus actionStatus;
        public final List<SagaError> actionErrors;
        public final Optional<UndoCommand<A>> undoCommand;
        public final boolean isUndo;

        @Override
        public <B> B cata(Function<SetInitialState<A>, B> f1, Function<SagaActionStateChanged<A>, B> f2, Function<SagaStatusChanged<A>, B> f3, Function<TransitionList<A>, B> f4) {
            return f2.apply(this);
        }
    }

    @Value(staticConstructor = "of")
    final class SagaStatusChanged<A> implements SagaStateTransition<A> {
        public final SagaId sagaId;
        public final SagaStatus sagaStatus;
        public final List<SagaError> sagaErrors;


        @Override
        public <B> B cata(Function<SetInitialState<A>, B> f1, Function<SagaActionStateChanged<A>, B> f2, Function<SagaStatusChanged<A>, B> f3, Function<TransitionList<A>, B> f4) {
            return f3.apply(this);
        }
    }

    @Value(staticConstructor = "of")
    final class TransitionList<A> implements SagaStateTransition<A> {
        public final List<SagaActionStateChanged<A>> actions;

        @Override
        public <B> B cata(Function<SetInitialState<A>, B> f1, Function<SagaActionStateChanged<A>, B> f2, Function<SagaStatusChanged<A>, B> f3, Function<TransitionList<A>, B> f4) {
            return f4.apply(this);
        }
    }

    /**
     * Catamorphism over SagaStateTransition
     */
    <B> B cata(
            Function<SetInitialState<A>, B> f1,
            Function<SagaActionStateChanged<A>, B> f2,
            Function<SagaStatusChanged<A>, B> f3,
            Function<TransitionList<A>, B> f4
            );
}
