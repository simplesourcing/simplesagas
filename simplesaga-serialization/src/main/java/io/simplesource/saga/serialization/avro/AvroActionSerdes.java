package io.simplesource.saga.serialization.avro;

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.simplesource.api.CommandId;
import io.simplesource.saga.model.action.ActionCommand;
import io.simplesource.saga.model.action.ActionId;
import io.simplesource.saga.model.messages.ActionRequest;
import io.simplesource.saga.model.messages.ActionResponse;
import io.simplesource.saga.model.action.UndoCommand;
import io.simplesource.saga.model.saga.SagaId;
import io.simplesource.saga.model.serdes.ActionSerdes;
import io.simplesource.saga.serialization.avro.generated.*;
import io.simplesource.saga.serialization.utils.SerdeUtils;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

import java.util.Optional;
import java.util.UUID;

final class AvroActionSerdes<A> implements ActionSerdes<A> {

    private final Serde<A> payloadSerde;
    private final Serde<AvroActionRequest> avroActionRequestSerde;
    private final Serde<AvroActionResponse> avroActionResponseSerde;

    public AvroActionSerdes(
            final Serde<A> payloadSerde,
            final String schemaRegistryUrl,
            final boolean useMockSchemaRegistry) {
        this.payloadSerde = payloadSerde;

        SchemaRegistryClient regClient = useMockSchemaRegistry ? new MockSchemaRegistryClient() : null;
        avroActionRequestSerde = SpecificSerdeUtils.specificAvroSerde(schemaRegistryUrl, false, regClient);
        avroActionResponseSerde = SpecificSerdeUtils.specificAvroSerde(schemaRegistryUrl, false, regClient);
    }

    @Override
    public Serde<SagaId> sagaId() {
        return SerdeUtils.iMap(Serdes.UUID(), SagaId::id, SagaId::of);
    }

    @Override
    public Serde<ActionId> actionId() {
        return SerdeUtils.iMap(Serdes.UUID(), ActionId::id, ActionId::of);
    }

    @Override
    public Serde<CommandId> commandId() {
        return SerdeUtils.iMap(Serdes.UUID(), CommandId::id, CommandId::of);
    }

    @Override
    public Serde<ActionRequest<A>> request() {
        return SerdeUtils.iMap(avroActionRequestSerde,
                (topic, r) -> AvroActionRequest.newBuilder()
                        .setActionId(r.actionId.toString())
                        .setSagaId(r.sagaId.toString())
                        .setActionCommand(SagaSerdeUtils.actionCommandToAvro(
                                payloadSerde,
                                topic,
                                r.actionCommand))
                        .setIsUndo(r.isUndo)
                        .build(),
                (topic, ar) -> {
                    AvroActionCommand aac = ar.getActionCommand();
                    ActionCommand<A> ac = SagaSerdeUtils.actionCommandFromAvro(payloadSerde, topic, aac);
                    return ActionRequest.of(SagaId.fromString(ar.getSagaId()),
                            ActionId.fromString(ar.getActionId()),
                            ac,
                            ar.getIsUndo());

                }
        );
    }

    @Override
    public Serde<ActionResponse<A>> response() {
        return SerdeUtils.iMap(avroActionResponseSerde,
                (topic, r) -> AvroActionResponse.newBuilder()
                        .setSagaId(r.sagaId.toString())
                        .setActionId(r.actionId.toString())
                        .setCommandId(r.commandId.id.toString())
                        .setIsUndo(r.isUndo)
                        .setResult(r.result.fold(SagaSerdeUtils::sagaErrorListToAvro,
                                optUac -> SagaSerdeUtils.optionalActionUndoCommandFromAvro(payloadSerde, topic, optUac)))
                        .build(),
                (topic, ar) -> ActionResponse.of(
                        SagaId.fromString(ar.getSagaId()),
                        ActionId.fromString(ar.getActionId()),
                        CommandId.of(UUID.fromString(ar.getCommandId())),
                        ar.getIsUndo(),
                        SagaSerdeUtils.<AvroActionUndoCommandOption, Optional<UndoCommand<A>>>sagaResultFromAvro(ar.getResult(),
                                aucOption -> SagaSerdeUtils.optionalActionUndoCommandFromAvro(payloadSerde, topic, aucOption))));
    }
}
