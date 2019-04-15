package io.simplesource.saga.model.serdes;

import io.simplesource.api.CommandId;
import io.simplesource.saga.model.action.ActionId;
import io.simplesource.saga.model.messages.ActionRequest;
import io.simplesource.saga.model.messages.ActionResponse;
import io.simplesource.saga.model.saga.SagaId;
import org.apache.kafka.common.serialization.Serde;

/**
 * The interface Action serdes.
 *
 * @param <A> the type parameter
 */
public interface ActionSerdes<A> {
    /**
     * Saga id serde.
     *
     * @return the serde
     */
    Serde<SagaId> sagaId();

    /**
     * Action id serde.
     *
     * @return the serde
     */
    Serde<ActionId> actionId();

    /**
     * Command id serde.
     *
     * @return the serde
     */
    Serde<CommandId> commandId();

    /**
     * Request serde.
     *
     * @return the serde
     */
    Serde<ActionRequest<A>> request();

    /**
     * Response serde.
     *
     * @return the serde
     */
    Serde<ActionResponse<A>> response();
}
