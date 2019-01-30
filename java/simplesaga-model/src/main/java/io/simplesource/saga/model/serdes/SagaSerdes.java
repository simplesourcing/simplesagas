package io.simplesource.saga.model.serdes;

import io.simplesource.saga.model.messages.SagaStateTransition;
import io.simplesource.saga.model.saga.Saga;
import org.apache.kafka.common.serialization.Serde;

public interface SagaSerdes<A> extends SagaClientSerdes<A> {
    Serde<Saga<A>> state();
    Serde<SagaStateTransition> transition();
}
