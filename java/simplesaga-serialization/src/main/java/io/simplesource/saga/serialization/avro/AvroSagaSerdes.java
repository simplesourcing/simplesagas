package io.simplesource.saga.serialization.avro;

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.simplesource.saga.model.messages.SagaStateTransition;
import io.simplesource.saga.model.saga.Saga;
import io.simplesource.saga.model.serdes.SagaSerdes;
import io.simplesource.saga.serialization.avro.generated.AvroSaga;
import io.simplesource.saga.serialization.utils.SerdeUtils;
import org.apache.kafka.common.serialization.Serde;

public class AvroSagaSerdes<A> extends AvroSagaClientSerdes<A> implements SagaSerdes<A> {

    private final Serde<AvroSaga> avroSagaSerde;

    AvroSagaSerdes(
            final Serde<A> payloadSerde,
            final String schemaRegistryUrl,
            final boolean useMockSchemaRegistry) {
        super(payloadSerde, schemaRegistryUrl, useMockSchemaRegistry);

        SchemaRegistryClient regClient = useMockSchemaRegistry ? new MockSchemaRegistryClient() : null;
        avroSagaSerde = SpecificSerdeUtils.specificAvroSerde(schemaRegistryUrl, false, regClient);
    }

    @Override
    public Serde<Saga<A>> state() {
        return SerdeUtils.iMap(avroSagaSerde, this::sagaToAvro, this::sagaFromAvro);
    }

    @Override
    public Serde<SagaStateTransition> transition() {
        return null;
    }
}
