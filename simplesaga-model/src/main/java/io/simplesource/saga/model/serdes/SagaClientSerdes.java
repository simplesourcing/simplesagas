package io.simplesource.saga.model.serdes;

import io.simplesource.saga.model.messages.SagaRequest;
import io.simplesource.saga.model.messages.SagaResponse;
import io.simplesource.saga.model.saga.SagaId;
import org.apache.kafka.common.serialization.Serde;

/**
 * The interface Saga client serdes.
 *
 * @param <A> the type parameter
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
