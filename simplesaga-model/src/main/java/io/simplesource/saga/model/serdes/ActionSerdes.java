package io.simplesource.saga.model.serdes;

import io.simplesource.api.CommandId;
import io.simplesource.saga.model.action.ActionId;
import io.simplesource.saga.model.messages.ActionRequest;
import io.simplesource.saga.model.messages.ActionResponse;
import io.simplesource.saga.model.saga.SagaId;
import org.apache.kafka.common.serialization.Serde;

public interface ActionSerdes<A> {
    Serde<SagaId> sagaId();
    Serde<ActionId> actionId();
    Serde<CommandId> commandId();
    Serde<ActionRequest<A>> request();
    Serde<ActionResponse<A>> response();
}
