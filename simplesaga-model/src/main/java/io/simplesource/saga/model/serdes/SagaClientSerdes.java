package io.simplesource.saga.model.serdes;

import io.simplesource.saga.model.messages.SagaRequest;
import io.simplesource.saga.model.messages.SagaResponse;
import io.simplesource.saga.model.saga.SagaId;
import org.apache.kafka.common.serialization.Serde;

/**
 * The serdes required to serialize and deserialize the saga request and response topics.
 *
 * @param <A> a representation of an action command that is shared across all actions in the saga. This is typically a generic type, such as Json, or if using Avro serialization, SpecificRecord or GenericRecord
 */
public interface SagaClientSerdes<A> {
    /**
     * Saga id serde.
     *
     * @return the serde
     */
    Serde<SagaId> sagaId();

    /**
     * Request serde.
     *
     * @return the serde
     */
    Serde<SagaRequest<A>> request();

    /**
     * Response serde.
     *
     * @return the serde
     */
    Serde<SagaResponse> response();
}
