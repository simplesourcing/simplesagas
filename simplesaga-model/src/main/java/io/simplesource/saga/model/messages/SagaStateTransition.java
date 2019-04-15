package io.simplesource.saga.model.messages;

import io.simplesource.saga.model.action.ActionId;
import io.simplesource.saga.model.action.ActionStatus;
import io.simplesource.saga.model.action.UndoCommand;
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
 * @param <A> A representation of an action command that is shared across all actions in the saga. This is typically a generic type, such as Json, or if using Avro serialization, SpecificRecord or GenericRecord
 */
public interface SagaStateTransition<A> {

    /**
     * A transition for setting the initial state of the Saga
     *
     * @param <A> A representation of an action command that is shared across all actions in the saga. This is typically a generic type, such as Json, or if using Avro serialization, SpecificRecord or GenericRecord
     */
    @Value(staticConstructor = "of")
    final class SetInitialState<A> implements SagaStateTransition<A> {
        public final Saga<A> sagaState;

        @Override
        public <B> B cata(Function<SetInitialState<A>, B> f1, Function<SagaActionStateChanged<A>, B> f2, Function<SagaStatusChanged<A>, B> f3, Function<TransitionList<A>, B> f4) {
            return f1.apply(this);
        }
    }

    /**
     * A transition representing state changes of a Saga caused by updates to state of an action
     *
     * @param <A> A representation of an action command that is shared across all actions in the saga. This is typically a generic type, such as Json, or if using Avro serialization, SpecificRecord or GenericRecord
     */
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

    /**
     * A transition representing state changes of a Saga caused by updating a sagas status
     *
     * @param <A> A representation of an action command that is shared across all actions in the saga. This is typically a generic type, such as Json, or if using Avro serialization, SpecificRecord or GenericRecord
     */
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

    /**
     * A transition representing multiple state changes to a saga
     *
     * @param <A> A representation of an action command that is shared across all actions in the saga. This is typically a generic type, such as Json, or if using Avro serialization, SpecificRecord or GenericRecord
     */
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
     *
     * @param <B> the target type of the catamorphism
     * @param f1 transition function based on saga initial state
     * @param f2 transition function based on change in action state
     * @param f3 transition function based on change in saga status
     * @param f4 transition function based on a list of transitions
     * @return the b
     */
    <B> B cata(
            Function<SetInitialState<A>, B> f1,
            Function<SagaActionStateChanged<A>, B> f2,
            Function<SagaStatusChanged<A>, B> f3,
            Function<TransitionList<A>, B> f4
            );
}
