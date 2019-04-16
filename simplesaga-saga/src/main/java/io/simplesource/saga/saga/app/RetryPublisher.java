package io.simplesource.saga.saga.app;

import io.simplesource.saga.model.messages.SagaStateTransition;
import io.simplesource.saga.model.saga.SagaId;

/**
 * A functional interface to support retrying action processing
 *
 * @param <A> a representation of an action command that is shared across all actions in the saga. This is typically a generic type, such as Json, or if using Avro serialization, SpecificRecord or GenericRecord
 */
@FunctionalInterface
public interface RetryPublisher<A> {
    void send(String topic, SagaId key, SagaStateTransition<A> value);
}
