package io.simplesource.saga.serialization.avro;

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.simplesource.saga.model.serdes.ActionSerdes;
import io.simplesource.saga.model.serdes.SagaClientSerdes;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Serde;


public class AvroSerdes {
    public static String PAYLOAD_TOPIC_SUFFIX = "-payload";

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

    static <A> SagaClientSerdes<A> sagaClientSerdes(
            final Serde<A> payloadSerde,
            final String schemaRegistryUrl,
            final boolean useMockSchemaRegistry) {
        return new AvroSagaClientSerdes<>(payloadSerde, schemaRegistryUrl, useMockSchemaRegistry);
    }

    static <A extends GenericRecord> SagaClientSerdes<A> sagaClientSerdes(
            final String schemaRegistryUrl,
            final boolean useMockSchemaRegistry) {
        SchemaRegistryClient regClient = useMockSchemaRegistry ? new MockSchemaRegistryClient() : null;
        return sagaClientSerdes(
                GenericSerdeUtils.genericAvroSerde(schemaRegistryUrl, false, regClient),
                schemaRegistryUrl,
                useMockSchemaRegistry);
    }
}
