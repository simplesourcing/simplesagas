package io.simplesource.saga.model.saga;

import io.simplesource.data.Sequence;
import io.simplesource.saga.model.action.ActionId;
import io.simplesource.saga.model.action.SagaAction;
import lombok.Value;

import java.util.*;

/**
 * The internal representation of a saga.
 *
 * @param <A> a representation of an action command that is shared across all actions in the saga. This is typically a generic type, such as Json, or if using Avro serialization, SpecificRecord or GenericRecord
 */
@Value
public class Saga<A> {
    /**
     * The Saga id uniquely identifies the saga.
     */
    public final SagaId sagaId;
    /**
     * Actions are stored in a map by {@code ActionId}. {@code ActionId} uniquely identifies the action (consisting of the command definition, the undo command definition if there is one and the action state).
     */
    public final Map<ActionId, SagaAction<A>> actions;
    /**
     * The status of the saga.
     */
    public final SagaStatus status;
    /**
     * If the saga fails, the cumulative list of errors that occurred in the processing for the saga.
     */
    public final List<SagaError> sagaError;
    /**
     * The sequence number of the saga, indicating the number of saga state transitions that have occurred.
     */
    public final Sequence sequence;

    /**
     * Static constructor for a Saga.
     * <p>
     * <i>It is suggested that clients do not define a saga directly through this method, but rather use the {@link io.simplesource.saga.client.dsl.SagaDSL SagaDSL} to build the saga.</i>
     *
     * @param <A> a representation of an action command that is shared across all actions in the saga. This is typically a generic type, such as Json, or if using Avro serialization, SpecificRecord or GenericRecord
     * @param sagaId   the saga id uniquely defines the saga
     * @param actions  a map of saga actions
     * @param status   the saga status
     * @param sequence the sequence number of the saga
     * @return the saga
     */
    public static <A> Saga<A> of(
            SagaId sagaId,
            Map<ActionId, SagaAction<A>> actions,
            SagaStatus status,
            Sequence sequence) {
        return new Saga<>(sagaId, actions, status, Collections.emptyList(), sequence);
    }

    public static <A> Saga<A> of(Map<ActionId, SagaAction<A>> actions) {
        return new Saga<>(SagaId.random(), actions, SagaStatus.NotStarted, Collections.emptyList(), Sequence.first());
    }

    /**
     * Creates a new saga instances with an updated saga status.
     *
     * @param status the status
     * @return the saga
     */
    public Saga<A> updated(SagaStatus status) {
        return updated(status, this.sagaError);
    }

    /**
     * Creates a new saga instances with an updated saga status and error list.
     *
     * @param status    the status
     * @param sagaError the saga error
     * @return the saga
     */
    public Saga<A> updated(SagaStatus status, List<SagaError> sagaError) {
        return updated(this.actions, status, sagaError);
    }

    /**
     * Creates a new saga instances with an updated set of actions, saga status and error list.
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
     * Creates a new saga instances with an updated set of actions and saga status.
     *
     * @param actions the actions
     * @param status  the status
     * @return the saga
     */
    public Saga<A> updated(Map<ActionId, SagaAction<A>> actions, SagaStatus status) {
        return new Saga<>(sagaId, actions, status, sagaError, sequence.next());
    }
}
