package io.simplesource.saga.saga.app;

import io.simplesource.saga.model.messages.SagaStateTransition;
import io.simplesource.saga.model.saga.SagaId;

@FunctionalInterface
public interface RetryPublisher<A> {
    void send(String topic, SagaId key, SagaStateTransition<A> value);
}


