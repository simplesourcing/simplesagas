package io.simplesource.saga.serialization.avro;

import io.simplesource.data.NonEmptyList;
import io.simplesource.data.Result;
import io.simplesource.kafka.serialization.util.GenericMapper;
import io.simplesource.saga.model.messages.ActionRequest;
import io.simplesource.saga.model.messages.ActionResponse;
import io.simplesource.saga.model.saga.SagaError;
import io.simplesource.saga.model.serdes.ActionSerdes;
import io.simplesource.saga.serialization.generated.avro.*;
import io.simplesource.saga.serialization.utils.SerdeUtils;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Arrays;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import static io.simplesource.kafka.serialization.avro.AvroSpecificGenericMapper.specificDomainMapper;

class AvroSerdes {
    static <A> ActionSerdes<A> actionSerdes(
            final GenericMapper<A, GenericRecord> keyMapper,
            final String schemaRegistryUrl,
            final boolean useMockSchemaRegistry) {
        return null;
    }

    static <A extends GenericRecord> ActionSerdes<A> actionSerdes(
            final String schemaRegistryUrl,
            final boolean useMockSchemaRegistry) {
        return actionSerdes(specificDomainMapper(),
                schemaRegistryUrl,
                useMockSchemaRegistry);
    }

    static class AvroActionSerdes<A> implements ActionSerdes<A> {

        private final String schemaRegistryUrl;
        private final boolean useMockSchemaRegistry;
        private final Serde<AvroActionId> actionIdSerde;
        private final Serde<AvroActionRequest> avroActionRequestSerde;
        private final Serde<AvroActionResponse> avroActionResponseSerde;

        public AvroActionSerdes(
                final GenericMapper<A, GenericRecord> keyMapper,
                final String schemaRegistryUrl,
                final boolean useMockSchemaRegistry) {

            this.schemaRegistryUrl = schemaRegistryUrl;
            this.useMockSchemaRegistry = useMockSchemaRegistry;
            actionIdSerde = SpecificSerdeUtils.specificAvroSerde(schemaRegistryUrl, true);
            avroActionRequestSerde = SpecificSerdeUtils.specificAvroSerde(schemaRegistryUrl, false);
            avroActionResponseSerde = SpecificSerdeUtils.specificAvroSerde(schemaRegistryUrl, false);
        }

        @Override
        public Serde<UUID> uuid() {
            return SerdeUtils.iMap(actionIdSerde, id -> new AvroActionId(id.toString()), aid -> UUID.fromString(aid.getId()));
        }

        @Override
        public Serde<ActionRequest<A>> request() {
            return null;
        }

        @Override
        public Serde<ActionResponse> response() {
            return SerdeUtils.iMap(avroActionResponseSerde,
                    r -> AvroActionResponse.newBuilder()
                            .setSagaId(r.sagaId.toString())
                            .setActionId(r.actionId.toString())
                            .setCommandId(r.commandId.toString())
                            .build(),
                    ar -> {
                        Object aRes = ar.getResult();
                        Result<SagaError, Boolean> result;
                        // TODO: remove the casting
                        if (aRes instanceof AvroSagaError[]) {
                            result = getSagaError((AvroSagaError[]) aRes);
                        } else if (aRes instanceof Boolean) {
                            result = Result.success((Boolean)aRes);
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

    private static Result<SagaError, Boolean> getSagaError(AvroSagaError[] aRes) {
        Result<SagaError, Boolean> result;
        AvroSagaError[] era = aRes;
        result = Result.failure(NonEmptyList.fromList(
                Arrays.stream(era)
                        .map(ae -> SagaError.of(SagaError.Reason.valueOf(ae.getReason()), ae.getMessage()))
                        .collect(Collectors.toList()))
                .orElse(NonEmptyList.of(
                        SagaError.of(SagaError.Reason.InternalError, "Serialization error"))));
        return result;
    }
}
