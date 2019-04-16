package io.simplesource.saga.model.serdes;

import io.simplesource.saga.model.messages.SagaStateTransition;
import io.simplesource.saga.model.saga.Saga;
import io.simplesource.saga.model.saga.SagaId;
import org.apache.kafka.common.serialization.Serde;

/**
 * The serdes required to serialize and deserialize the saga state and state transition topics, as well as the request and response topics.
 *
 * @param <A> a representation of an action command that is shared across all actions in the saga. This is typically a generic type, such as Json, or if using Avro serialization, SpecificRecord or GenericRecord
 */
public interface SagaSerdes<A> extends SagaClientSerdes<A> {
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
