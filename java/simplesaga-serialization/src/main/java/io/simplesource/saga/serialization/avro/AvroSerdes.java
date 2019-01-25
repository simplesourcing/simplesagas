package io.simplesource.saga.serialization.avro;

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.simplesource.data.NonEmptyList;
import io.simplesource.data.Result;
import io.simplesource.saga.model.action.ActionCommand;
import io.simplesource.saga.model.messages.ActionRequest;
import io.simplesource.saga.model.messages.ActionResponse;
import io.simplesource.saga.model.saga.SagaError;
import io.simplesource.saga.model.serdes.ActionSerdes;
import io.simplesource.saga.serialization.avro.generated.*;
import io.simplesource.saga.serialization.utils.SerdeUtils;
import io.simplesource.saga.shared.utils.StreamAppUtils;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Serde;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

class AvroSerdes {
    static String PAYLOAD_TOPIC_SUFFIX = "-payload";

    static <A> ActionSerdes<A> actionSerdes(
            final Serde<A> payloadSerde,
            final String schemaRegistryUrl,
            final boolean useMockSchemaRegistry) {
        return new AvroActionSerdes<>(payloadSerde, schemaRegistryUrl, useMockSchemaRegistry);
    }

    static <A extends GenericRecord> ActionSerdes<A> actionSerdes(
            final String schemaRegistryUrl,
            final boolean useMockSchemaRegistry) {
        SchemaRegistryClient regClient = useMockSchemaRegistry ? new MockSchemaRegistryClient() : null;
        return actionSerdes(
                GenericSerdeUtils.genericAvroSerde(schemaRegistryUrl, false, regClient),
                schemaRegistryUrl,
                useMockSchemaRegistry);
    }

    static class AvroActionSerdes<A> implements ActionSerdes<A> {

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

        private static Object apply(Object e) {
            return (AvroSagaError) e;
        }

        @Override
        public Serde<UUID> uuid() {
            return SerdeUtils.iMap(actionIdSerde, id -> new AvroActionId(id.toString()), aid -> UUID.fromString(aid.getId()));
        }

        @Override
        public Serde<ActionRequest<A>> request() {
            return SerdeUtils.iMap(avroActionRequestSerde,
                    (topic, r) -> {
                        byte[] serializedPayload = payloadSerde.serializer().serialize(topic + PAYLOAD_TOPIC_SUFFIX, r.actionCommand.command);
                        AvroActionCommand aac = AvroActionCommand
                                .newBuilder()
                                .setCommandId(r.actionCommand.commandId.toString())
                                .setCommand(ByteBuffer.wrap(serializedPayload))
                                .build();
                        return AvroActionRequest.newBuilder()
                                .setActionId(r.actionId().toString())
                                .setSagaId(r.sagaId().toString())
                                .setActionType(r.actionType())
                                .setActionCommand(aac)
                                .build();
                    },
                    (topic, ar) -> {
                        AvroActionCommand aac = ar.getActionCommand();
                        ByteBuffer spf = aac.getCommand();
                        A payload = payloadSerde.deserializer().deserialize(topic + PAYLOAD_TOPIC_SUFFIX, spf.array());
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
                            .setResult(r.result.fold(
                                    es -> es.map(e ->
                                            new AvroSagaError(
                                                    e.getReason().toString(),
                                                    e.getMessage()))
                                            .toList(),
                                    Boolean::valueOf))
                            .build(),
                    ar -> {
                        Object aRes = ar.getResult();
                        Result<SagaError, Boolean> result;

                        // TODO: remove the casting
                        if (aRes instanceof GenericArray) {
                            GenericArray<Object> v = (GenericArray) aRes;
                            Stream<AvroSagaError> avroErrors = v.stream()
                                    .map(x -> (AvroSagaError) x)
                                    .filter(Objects::nonNull);
                            result = getSagaError(avroErrors);
                        } else if (aRes instanceof Boolean) {
                            result = Result.success((Boolean) aRes);
                        } else {
                            result = Result.failure(SagaError.of(SagaError.Reason.InternalError, "Serialization error"));
                        }

                        return new ActionResponse(
                                UUID.fromString(ar.getSagaId()),
                                UUID.fromString(ar.getActionId()),
                                UUID.fromString(ar.getCommandId()),
                                result
                        );
                    });
        }
    }

    private static Result<SagaError, Boolean> getSagaError(Stream<AvroSagaError> aRes) {
        return Result.failure(NonEmptyList.fromList(
                aRes
                        .map(ae -> SagaError.of(SagaError.Reason.valueOf(ae.getReason()), ae.getMessage()))
                        .collect(Collectors.toList()))
                .orElse(NonEmptyList.of(
                        SagaError.of(SagaError.Reason.InternalError, "Serialization error"))));
    }
}
