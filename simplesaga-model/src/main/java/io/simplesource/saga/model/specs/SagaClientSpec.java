package io.simplesource.saga.model.specs;

import io.simplesource.saga.model.serdes.SagaClientSerdes;
import lombok.Value;

/**
 * Represents the details required to create a saga client instance
 *
 * @param <A> a representation of an action command that is shared across all actions in the saga. This is typically a generic type, such as Json, or if using Avro serialization, SpecificRecord or GenericRecord
 */
@Value(staticConstructor = "of")
public class SagaClientSpec<A> {
    /**
     * The serdes required for all the saga request and response topics.
     */
    public final SagaClientSerdes<A> serdes;
}
