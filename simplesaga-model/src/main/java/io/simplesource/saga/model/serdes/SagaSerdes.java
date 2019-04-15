package io.simplesource.saga.model.serdes;

import io.simplesource.saga.model.messages.SagaStateTransition;
import io.simplesource.saga.model.saga.Saga;
import io.simplesource.saga.model.saga.SagaId;
import org.apache.kafka.common.serialization.Serde;

/**
 * The interface Saga serdes.
 *
 * @param <A> the type parameter
 */
public interface SagaSerdes<A> extends SagaClientSerdes<A> {
    Serde<SagaId> sagaId();

    /**
     * State serde.
     *
     * @return the serde
     */
    Serde<Saga<A>> state();

    /**
     * Transition serde.
     *
     * @return the serde
     */
    Serde<SagaStateTransition<A>> transition();
}
