package io.simplesource.saga.model.serdes;

import io.simplesource.saga.model.messages.SagaRequest;
import io.simplesource.saga.model.messages.SagaResponse;
import io.simplesource.saga.model.saga.Saga;
import org.apache.kafka.common.serialization.Serde;

import java.util.UUID;

public interface SagaClientSerdes<A> {
    Serde<UUID> uuid();
    Serde<SagaRequest<A>> request();
    Serde<SagaResponse> response();
}
