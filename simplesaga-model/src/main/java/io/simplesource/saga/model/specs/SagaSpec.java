package io.simplesource.saga.model.specs;

import io.simplesource.kafka.spec.WindowSpec;
import io.simplesource.saga.model.serdes.SagaSerdes;
import lombok.Value;

/**
 * Represents the details required to create a saga coordinator
 *
 * @param <A> a representation of an action command that is shared across all actions in the saga. This is typically a generic type, such as Json, or if using Avro serialization, SpecificRecord or GenericRecord
 */
@Value(staticConstructor = "of")
public class SagaSpec<A> {
    /**
     * The serdes required for all the saga coordinator topics (request and response as well as state and state transition topics).
     */
    public final SagaSerdes<A> serdes;
    /**
     * Represents the response window for the Saga API. This defines how long the saga API should be able to access the results of any saga in execution or previously completed saga.
     * @see io.simplesource.saga.model.api.SagaAPI
     */
    public final WindowSpec responseWindow;
}

