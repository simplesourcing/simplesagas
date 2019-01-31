package io.simplesource.saga.serialization.avro;

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.simplesource.saga.model.serdes.ActionSerdes;
import io.simplesource.saga.model.serdes.SagaClientSerdes;
import io.simplesource.saga.model.serdes.SagaSerdes;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.serialization.Serde;


public class AvroSerdes {
    public static String PAYLOAD_TOPIC_SUFFIX = "-payload";

    public static <A> ActionSerdes<A> actionSerdes(
            final Serde<A> payloadSerde,
            final String schemaRegistryUrl,
            final boolean useMockSchemaRegistry) {
        return new AvroActionSerdes<>(payloadSerde, schemaRegistryUrl, useMockSchemaRegistry);
    }
//
//    public static <A extends GenericRecord> ActionSerdes<A> actionSerdes(
//            final String schemaRegistryUrl,
//            final boolean useMockSchemaRegistry) {
//        SchemaRegistryClient regClient = useMockSchemaRegistry ? new MockSchemaRegistryClient() : null;
//        return actionSerdes(
//                GenericSerdeUtils.genericAvroSerde(schemaRegistryUrl, false, regClient),
//                schemaRegistryUrl,
//                useMockSchemaRegistry);
//    }

    public static <A extends SpecificRecord> ActionSerdes<A> actionSerdes(
            final String schemaRegistryUrl,
            final boolean useMockSchemaRegistry) {
        SchemaRegistryClient regClient = useMockSchemaRegistry ? new MockSchemaRegistryClient() : null;
        return actionSerdes(
                SpecificSerdeUtils.specificAvroSerde(schemaRegistryUrl, false, regClient),
                schemaRegistryUrl,
                useMockSchemaRegistry);
    }

    public static <A> SagaClientSerdes<A> sagaClientSerdes(
            final Serde<A> payloadSerde,
            final String schemaRegistryUrl,
            final boolean useMockSchemaRegistry) {
        return new AvroSagaClientSerdes<>(payloadSerde, schemaRegistryUrl, useMockSchemaRegistry);
    }
//
//    public static <A extends GenericRecord> SagaClientSerdes<A> sagaClientSerdes(
//            final String schemaRegistryUrl,
//            final boolean useMockSchemaRegistry) {
//        SchemaRegistryClient regClient = useMockSchemaRegistry ? new MockSchemaRegistryClient() : null;
//        return sagaClientSerdes(
//                GenericSerdeUtils.genericAvroSerde(schemaRegistryUrl, false, regClient),
//                schemaRegistryUrl,
//                useMockSchemaRegistry);
//    }

    public static <A extends SpecificRecord> SagaClientSerdes<A> sagaClientSerdes(
            final String schemaRegistryUrl,
            final boolean useMockSchemaRegistry) {
        SchemaRegistryClient regClient = useMockSchemaRegistry ? new MockSchemaRegistryClient() : null;
        return sagaClientSerdes(
                SpecificSerdeUtils.specificAvroSerde(schemaRegistryUrl, false, regClient),
                schemaRegistryUrl,
                useMockSchemaRegistry);
    }

    public static <A extends SpecificRecord> SagaSerdes<A> sagaSerdes(
            final Serde<A> payloadSerde,
            final String schemaRegistryUrl,
            final boolean useMockSchemaRegistry) {
        SchemaRegistryClient regClient = useMockSchemaRegistry ? new MockSchemaRegistryClient() : null;
        AvroSagaSerdes<A> serdes = new AvroSagaSerdes<>(payloadSerde, schemaRegistryUrl, useMockSchemaRegistry);
        return serdes;
    }

    public static <A extends SpecificRecord> SagaSerdes<A> sagaSerdes(
            final String schemaRegistryUrl,
            final boolean useMockSchemaRegistry) {
        SchemaRegistryClient regClient = useMockSchemaRegistry ? new MockSchemaRegistryClient() : null;
        return sagaSerdes(
                SpecificSerdeUtils.specificAvroSerde(schemaRegistryUrl, false, regClient),
                schemaRegistryUrl,
                useMockSchemaRegistry);
    }

}
