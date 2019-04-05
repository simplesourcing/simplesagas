package io.simplesource.saga.shared.kafka;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serde;

import java.util.Properties;
import java.util.function.Function;

public class KafkaUtils {
    static <K, V> ProducerRecord<byte[], byte[]> toBytes(
            ProducerRecord<K, V> record,
            Serde<K> kSerde,
            Serde<V> vSerde) {
        String topicName = record.topic();
        byte[] kb = kSerde.serializer().serialize(topicName, record.key());
        byte[] kv = vSerde.serializer().serialize(topicName, record.value());
        return new ProducerRecord<>(topicName, kb, kv);
    }

    public static Properties copyProperties(Properties properties) {
        Properties newProps = new Properties();
        properties.forEach((key, value) -> newProps.setProperty(key.toString(), value.toString()));
        return newProps;
    };
}
