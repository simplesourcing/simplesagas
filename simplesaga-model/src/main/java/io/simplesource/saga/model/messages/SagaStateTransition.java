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

/**
 * SagaStateTransition interface. Represents transitions of saga state
 *
 * @param <A> the type parameter
 */
public interface SagaStateTransition<A> {

    /**
     * A transition for setting the initial state of the Saga
     *
     * @param <A> the type parameter
     */
    @Value(staticConstructor = "of")
    final class SetInitialState<A> implements SagaStateTransition<A> {
        /**
         * The Saga state.
         */
        public final Saga<A> sagaState;

        @Override
        public <B> B cata(Function<SetInitialState<A>, B> f1, Function<SagaActionStateChanged<A>, B> f2, Function<SagaStatusChanged<A>, B> f3, Function<TransitionList<A>, B> f4) {
            return f1.apply(this);
        }
    }

    /**
     * A transition representing state changes of a Saga caused by updates to state of an action
     *
     * @param <A> the type parameter
     */
    @Value(staticConstructor = "of")
    final class SagaActionStateChanged<A> implements SagaStateTransition<A> {
        /**
         * The Saga id.
         */
        public final SagaId sagaId;
        /**
         * The Action id.
         */
        public final ActionId actionId;
        /**
         * The Action status.
         */
        public final ActionStatus actionStatus;
        /**
         * The Action errors.
         */
        public final List<SagaError> actionErrors;
        /**
         * The Undo command.
         */
        public final Optional<UndoCommand<A>> undoCommand;
        /**
         * The Is undo.
         */
        public final boolean isUndo;

        @Override
        public <B> B cata(Function<SetInitialState<A>, B> f1, Function<SagaActionStateChanged<A>, B> f2, Function<SagaStatusChanged<A>, B> f3, Function<TransitionList<A>, B> f4) {
            return f2.apply(this);
        }
    }

    /**
     * A transition representing state changes of a Saga caused by updating a sagas status
     *
     * @param <A> the type parameter
     */
    @Value(staticConstructor = "of")
    final class SagaStatusChanged<A> implements SagaStateTransition<A> {
        /**
         * The Saga id.
         */
        public final SagaId sagaId;
        /**
         * The Saga status.
         */
        public final SagaStatus sagaStatus;
        /**
         * The Saga errors.
         */
        public final List<SagaError> sagaErrors;


        @Override
        public <B> B cata(Function<SetInitialState<A>, B> f1, Function<SagaActionStateChanged<A>, B> f2, Function<SagaStatusChanged<A>, B> f3, Function<TransitionList<A>, B> f4) {
            return f3.apply(this);
        }
    }

    /**
     * A transition representing multiple state changes to a saga
     *
     * @param <A> the type parameter
     */
    @Value(staticConstructor = "of")
    final class TransitionList<A> implements SagaStateTransition<A> {
        /**
         * The Actions.
         */
        public final List<SagaActionStateChanged<A>> actions;

        @Override
        public <B> B cata(Function<SetInitialState<A>, B> f1, Function<SagaActionStateChanged<A>, B> f2, Function<SagaStatusChanged<A>, B> f3, Function<TransitionList<A>, B> f4) {
            return f4.apply(this);
        }
    }

    /**
     * Catamorphism over SagaStateTransition
     *
     * @param <B> the target type of the catamorphism
     * @param f1  the f 1
     * @param f2  the f 2
     * @param f3  the f 3
     * @param f4  the f 4
     * @return the b
     */
    <B> B cata(
            Function<SetInitialState<A>, B> f1,
            Function<SagaActionStateChanged<A>, B> f2,
            Function<SagaStatusChanged<A>, B> f3,
            Function<TransitionList<A>, B> f4
            );
}
