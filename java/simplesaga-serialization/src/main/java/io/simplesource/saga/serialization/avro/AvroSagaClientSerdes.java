package io.simplesource.saga.serialization.avro;

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.simplesource.data.Sequence;
import io.simplesource.saga.model.messages.SagaRequest;
import io.simplesource.saga.model.messages.SagaResponse;
import io.simplesource.saga.model.serdes.SagaClientSerdes;
import io.simplesource.saga.serialization.avro.generated.*;
import io.simplesource.saga.serialization.utils.SerdeUtils;
import org.apache.kafka.common.serialization.Serde;

import java.util.UUID;

public class AvroSagaClientSerdes<A> implements SagaClientSerdes<A> {

    private final Serde<A> payloadSerde;
    private final Serde<AvroSagaId> avroSagaIdSerde;
    private final Serde<AvroSagaRequest> avroSagaRequestSerde;
    private final Serde<AvroSagaResponse> avroSagaResponseSerde;

    public AvroSagaClientSerdes(
            final Serde<A> payloadSerde,
            final String schemaRegistryUrl,
            final boolean useMockSchemaRegistry) {
        this.payloadSerde = payloadSerde;

        SchemaRegistryClient regClient = useMockSchemaRegistry ? new MockSchemaRegistryClient() : null;
        avroSagaIdSerde = SpecificSerdeUtils.specificAvroSerde(schemaRegistryUrl, true, regClient);
        avroSagaRequestSerde = SpecificSerdeUtils.specificAvroSerde(schemaRegistryUrl, false, regClient);
        avroSagaResponseSerde = SpecificSerdeUtils.specificAvroSerde(schemaRegistryUrl, false, regClient);
    }

    @Override
    public Serde<UUID> uuid() {
        return SerdeUtils.iMap(avroSagaIdSerde, id -> new AvroSagaId(id.toString()), aid -> UUID.fromString(aid.getId()));
    }

    @Override
    public Serde<SagaRequest<A>> request() {
        return null;
    }

    @Override
    public Serde<SagaResponse> response() {
        return SerdeUtils.iMap(avroSagaResponseSerde,
                r -> AvroSagaResponse.newBuilder()
                        .setSagaId(r.sagaId.toString())
                        .setResult(r.result.fold(
                                es -> es.map(e ->
                                        new AvroSagaError(
                                                e.getReason().toString(),
                                                e.getMessage()))
                                        .toList(),
                                Sequence::getSeq))
                        .build(),
                ar -> new SagaResponse(
                        UUID.fromString(ar.getSagaId()),
                        SagaSerdeUtils.<Long, Sequence>getSagaResult(ar.getResult(), x -> Sequence.position(x))));

    }
}
