package io.simplesource.saga.serialization.avro;

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.simplesource.saga.model.action.ActionCommand;
import io.simplesource.saga.model.messages.ActionRequest;
import io.simplesource.saga.model.messages.ActionResponse;
import io.simplesource.saga.model.serdes.ActionSerdes;
import io.simplesource.saga.serialization.avro.generated.*;
import io.simplesource.saga.serialization.utils.SerdeUtils;
import org.apache.kafka.common.serialization.Serde;

import java.nio.ByteBuffer;
import java.util.UUID;

public class AvroActionSerdes<A> implements ActionSerdes<A> {

    private final Serde<A> payloadSerde;
    private final Serde<AvroActionId> actionIdSerde;
    private final Serde<AvroActionRequest> avroActionRequestSerde;
    private final Serde<AvroActionResponse> avroActionResponseSerde;

    public AvroActionSerdes(
            final Serde<A> payloadSerde,
            final String schemaRegistryUrl,
            final boolean useMockSchemaRegistry) {
        this.payloadSerde = payloadSerde;

        SchemaRegistryClient regClient = useMockSchemaRegistry ? new MockSchemaRegistryClient() : null;
        actionIdSerde = SpecificSerdeUtils.specificAvroSerde(schemaRegistryUrl, true, regClient);
        avroActionRequestSerde = SpecificSerdeUtils.specificAvroSerde(schemaRegistryUrl, false, regClient);
        avroActionResponseSerde = SpecificSerdeUtils.specificAvroSerde(schemaRegistryUrl, false, regClient);
    }

    @Override
    public Serde<UUID> uuid() {
        return SerdeUtils.iMap(actionIdSerde, id -> new AvroActionId(id.toString()), aid -> UUID.fromString(aid.getId()));
    }

    @Override
    public Serde<ActionRequest<A>> request() {
        return SerdeUtils.iMap(avroActionRequestSerde,
                (topic, r) -> AvroActionRequest.newBuilder()
                        .setActionId(r.actionId().toString())
                        .setSagaId(r.sagaId().toString())
                        .setActionType(r.actionType())
                        .setActionCommand(SagaSerdeUtils.actionCommandToAvro(
                                payloadSerde,
                                topic + AvroSerdes.PAYLOAD_TOPIC_SUFFIX,
                                r.actionCommand))
                        .build(),
                (topic, ar) -> {
                    AvroActionCommand aac = ar.getActionCommand();
                    ByteBuffer spf = aac.getCommand();
                    A payload = payloadSerde.deserializer().deserialize(topic + AvroSerdes.PAYLOAD_TOPIC_SUFFIX, spf.array());
                    ActionCommand<A> ac = new ActionCommand<>(UUID.fromString(aac.getCommandId()), payload);
                    return ActionRequest.<A>builder()
                            .sagaId(UUID.fromString(ar.getSagaId()))
                            .actionId(UUID.fromString(ar.getActionId()))
                            .actionCommand(ac)
                            .actionType(ar.getActionType())
                            .build();
                }
        );
    }

    @Override
    public Serde<ActionResponse> response() {
        return SerdeUtils.iMap(avroActionResponseSerde,
                r -> AvroActionResponse.newBuilder()
                        .setSagaId(r.sagaId.toString())
                        .setActionId(r.actionId.toString())
                        .setCommandId(r.commandId.toString())
                        .setResult(r.result.fold(SagaSerdeUtils::sagaErrorListToAvro, x -> x))
                        .build(),
                ar -> new ActionResponse(
                        UUID.fromString(ar.getSagaId()),
                        UUID.fromString(ar.getActionId()),
                        UUID.fromString(ar.getCommandId()),
                        SagaSerdeUtils.<Boolean, Boolean>sagaResultFromAvro(ar.getResult(), x -> x)));
    }
}
