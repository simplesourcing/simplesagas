package io.simplesource.saga.model.serdes;

import io.simplesource.saga.model.messages.SagaRequest;
import io.simplesource.saga.model.messages.SagaResponse;
import io.simplesource.saga.model.saga.SagaId;
import org.apache.kafka.common.serialization.Serde;

public interface SagaClientSerdes<A> {
    Serde<SagaId> sagaId();
    Serde<SagaRequest<A>> request();
    Serde<SagaResponse> response();
}
