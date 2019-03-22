package io.simplesource.saga.model.serdes;

import io.simplesource.api.CommandId;
import io.simplesource.saga.model.messages.ActionRequest;
import io.simplesource.saga.model.messages.ActionResponse;
import org.apache.kafka.common.serialization.Serde;

import java.util.UUID;

public interface ActionSerdes<A> {
    Serde<UUID> uuid();
    Serde<CommandId> commandId();
    Serde<ActionRequest<A>> request();
    Serde<ActionResponse> response();
}
