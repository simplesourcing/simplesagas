package io.simplesource.saga.action.internal;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serde;

final class AsyncKafkaPublisher<K, V> implements AsyncPublisher<K, V> {
    private final KafkaProducer<byte[], byte[]> producer;
    private final Serde<K> keySerdes;
    private final Serde<V> valueSerdes;

    AsyncKafkaPublisher(KafkaProducer<byte[], byte[]> kafkaProducer, Serde<K> keySerdes, Serde<V> valueSerdes) {
        producer = kafkaProducer;
        this.keySerdes = keySerdes;
        this.valueSerdes = valueSerdes;
    }

    public void send(String topic, K key, V value) {
        ProducerRecord<K, V> outputRecord = new ProducerRecord<>(topic, key, value);
        ProducerRecord<byte[], byte[]> byteRecord = AsyncProcessor.toBytes(outputRecord, keySerdes, valueSerdes);
        producer.send(byteRecord);
    }
}
