package io.simplesource.saga.model.saga;

import io.simplesource.data.Sequence;
import io.simplesource.saga.model.action.ActionId;
import io.simplesource.saga.model.action.SagaAction;
import lombok.Value;

import java.util.*;

/**
 * The type Saga.
 *
 * @param <A> the type parameter
 */
@Value
public class Saga<A> {
    /**
     * The Saga id.
     */
    public final SagaId sagaId;
    /**
     * The Actions.
     */
    public final Map<ActionId, SagaAction<A>> actions;
    /**
     * The Status.
     */
    public final SagaStatus status;
    /**
     * The Saga error.
     */
    public final List<SagaError> sagaError;
    /**
     * The Sequence.
     */
    public final Sequence sequence;

    /**
     * Of saga.
     *
     * @param <A>      the type parameter
     * @param sagaId   the saga id
     * @param actions  the actions
     * @param status   the status
     * @param sequence the sequence
     * @return the saga
     */
    public static <A> Saga<A> of(
            SagaId sagaId,
            Map<ActionId, SagaAction<A>> actions,
            SagaStatus status,
            Sequence sequence) {
        return new Saga<>(sagaId, actions, status, Collections.emptyList(), sequence);
    }

    /**
     * Of saga.
     *
     * @param <A>     the type parameter
     * @param actions the actions
     * @return the saga
     */
    public static <A> Saga<A> of(Map<ActionId, SagaAction<A>> actions) {
        return new Saga<>(SagaId.random(), actions, SagaStatus.NotStarted, Collections.emptyList(), Sequence.first());
    }

    /**
     * Updated saga.
     *
     * @param status the status
     * @return the saga
     */
    public Saga<A> updated(SagaStatus status) {
        return updated(status, this.sagaError);
    }

    /**
     * Updated saga.
     *
     * @param status    the status
     * @param sagaError the saga error
     * @return the saga
     */
    public Saga<A> updated(SagaStatus status, List<SagaError> sagaError) {
        return updated(this.actions, status, sagaError);
    }

    /**
     * Updated saga.
     *
     * @param actions   the actions
     * @param status    the status
     * @param sagaError the saga error
     * @return the saga
     */
    public Saga<A> updated(Map<ActionId, SagaAction<A>> actions, SagaStatus status, List<SagaError> sagaError) {
        return new Saga<>(sagaId, actions, status, sagaError, sequence.next());
    }

    /**
     * Updated saga.
     *
     * @param actions the actions
     * @param status  the status
     * @return the saga
     */
    public Saga<A> updated(Map<ActionId, SagaAction<A>> actions, SagaStatus status) {
        return new Saga<>(sagaId, actions, status, sagaError, sequence.next());
    }
}
