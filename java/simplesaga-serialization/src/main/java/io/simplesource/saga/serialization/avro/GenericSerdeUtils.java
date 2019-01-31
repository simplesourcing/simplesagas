//package io.simplesource.saga.serialization.avro;
//
//import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
//import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
//import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
//import org.apache.avro.generic.GenericRecord;
//import org.apache.kafka.common.serialization.Serde;
//
//import java.util.HashMap;
//import java.util.Map;
//
//public class GenericSerdeUtils {
//
//    public static <T extends GenericRecord> Serde<T> genericAvroSerde(String hostName, Boolean isSerdeForRecordKeys) {
//        return genericAvroSerde(hostName, isSerdeForRecordKeys, null);
//    }
//
//    public static <T extends GenericRecord> Serde<T> genericAvroSerde(String registryUrl, Boolean isSerdeForRecordKeys, SchemaRegistryClient registryClient) {
//        final Map<String, Object> configMap = new HashMap<>();
//        configMap.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, registryUrl);
//        final Serde<T> serde = (Serde<T>)(registryClient != null ? new GenericAvroSerde(registryClient) : new GenericAvroSerde());
//        serde.configure(configMap, isSerdeForRecordKeys);
//        return serde;
//    }
//}
//
