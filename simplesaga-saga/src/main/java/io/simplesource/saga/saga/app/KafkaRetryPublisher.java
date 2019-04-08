package io.simplesource.saga.saga.app;

import io.simplesource.saga.model.messages.SagaStateTransition;
import io.simplesource.saga.model.saga.SagaId;
import io.simplesource.saga.shared.kafka.KafkaUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serde;

public final class KafkaRetryPublisher<A> implements RetryPublisher<A> {
    private final KafkaProducer<byte[], byte[]> producer;
    private final Serde<SagaId> keySerdes;
    private final Serde<SagaStateTransition<A>> valueSerdes;

    public KafkaRetryPublisher(KafkaProducer<byte[], byte[]> kafkaProducer, Serde<SagaId> keySerdes, Serde<SagaStateTransition<A>> valueSerdes) {
        producer = kafkaProducer;
        this.keySerdes = keySerdes;
        this.valueSerdes = valueSerdes;
    }

    public void send(String topic, SagaId key, SagaStateTransition<A> value) {
        ProducerRecord<SagaId, SagaStateTransition<A>> outputRecord = new ProducerRecord<>(topic, key, value);
        ProducerRecord<byte[], byte[]> byteRecord = KafkaUtils.toBytes(outputRecord, keySerdes, valueSerdes);
        producer.send(byteRecord);
    }
}
