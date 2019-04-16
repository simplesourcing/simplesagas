package io.simplesource.saga.model.serdes;

import io.simplesource.api.CommandId;
import io.simplesource.saga.model.action.ActionId;
import io.simplesource.saga.model.messages.ActionRequest;
import io.simplesource.saga.model.messages.ActionResponse;
import io.simplesource.saga.model.saga.SagaId;
import org.apache.kafka.common.serialization.Serde;

/**
 * The serdes required to serialize and deserialize the action request and response topics.
 *
 * @param <A> a representation of an action command that is shared across all actions in the saga. This is typically a generic type, such as Json, or if using Avro serialization, SpecificRecord or GenericRecord
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
     * Serde for an action request.
     *
     * @return the serde
     */
    Serde<ActionRequest<A>> request();

    /**
     * Serde for an action response.
     *
     * @return the serde
     */
    Serde<ActionResponse<A>> response();
}
