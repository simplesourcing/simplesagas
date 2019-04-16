package io.simplesource.saga.shared.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serde;

public final class KafkaPublisher<K, V> {
    private final KafkaProducer<byte[], byte[]> producer;
    private final Serde<K> keySerdes;
    private final Serde<V> valueSerdes;

    public KafkaPublisher(KafkaProducer<byte[], byte[]> kafkaProducer, Serde<K> keySerdes, Serde<V> valueSerdes) {
        producer = kafkaProducer;
        this.keySerdes = keySerdes;
        this.valueSerdes = valueSerdes;
    }

    public void send(String topic, K key, V value) {
        ProducerRecord<K, V> outputRecord = new ProducerRecord<>(topic, key, value);
        ProducerRecord<byte[], byte[]> byteRecord = toBytes(outputRecord, keySerdes, valueSerdes);
        producer.send(byteRecord);
    }

    private static <K, V> ProducerRecord<byte[], byte[]> toBytes(
            ProducerRecord<K, V> record,
            Serde<K> kSerde,
            Serde<V> vSerde) {
        String topicName = record.topic();
        byte[] kb = kSerde.serializer().serialize(topicName, record.key());
        byte[] kv = vSerde.serializer().serialize(topicName, record.value());
        return new ProducerRecord<>(topicName, kb, kv);
    }
}
