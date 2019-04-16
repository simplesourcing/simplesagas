package io.simplesource.saga.model.messages;

import io.simplesource.saga.model.saga.Saga;
import io.simplesource.saga.model.saga.SagaId;
import lombok.Value;

/**
 * A request to execute a saga, published by client (either directly, or via the {@link io.simplesource.saga.model.api.SagaAPI SagaAPI} in the
 * saga request topic, and consumed by the saga coordinator.
 *
 * @param <A> a representation of an action command that is shared across all actions in the saga. This is typically a generic type, such as Json, or if using Avro serialization, SpecificRecord or GenericRecord
 */
@Value(staticConstructor = "of")
public class SagaRequest<A> {
    /**
     * The saga id uniquely identifies the saga, and is used as the key for the saga request and responses.
     */
    public final SagaId sagaId;
    /**
     * The initial state of the saga. It is suggested to use the saga builder DSL to create the saga definition.
     */
    public final Saga<A> initialState;
}
