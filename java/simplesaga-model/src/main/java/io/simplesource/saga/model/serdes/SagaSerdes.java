package io.simplesource.saga.model.serdes;

import io.simplesource.saga.model.messages.SagaStateTransition;
import io.simplesource.saga.model.saga.Saga;
import io.simplesource.saga.model.saga.SagaId;
import org.apache.kafka.common.serialization.Serde;

public interface SagaSerdes<A> extends SagaClientSerdes<A> {
    Serde<SagaId> sagaId();
    Serde<Saga<A>> state();
    Serde<SagaStateTransition> transition();
}
