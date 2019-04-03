package io.simplesource.saga.shared.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.Serde;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

public final class ConsumerRunner<K, V> implements Runnable {

    private final AtomicBoolean closed;
    private Optional<KafkaConsumer<K, V>> consumer = Optional.empty();
    private final Logger logger = LoggerFactory.getLogger(ConsumerRunner.class);
    private final Properties consumerConfig;
    private final Consumer<Boolean> onClose;

    private final BiConsumer<K, V> processor;
    private final Serde<K> keySerde;
    private final Serde< V> valueSerde;
    private final String topicName;
    
    public ConsumerRunner(
            Properties consumerConfig,
            BiConsumer<K, V> processor,
            Serde<K> keySerde,
                    Serde< V> valueSerde,
                    String topicName,
            Consumer<Boolean> onClose) {
        this.closed = new AtomicBoolean(false);
        this.consumerConfig = consumerConfig;
        this.onClose = onClose;

        this.processor = processor;
        this.keySerde = keySerde;
        this.valueSerde = valueSerde;
        this.topicName = topicName;
    }
    
    @Override
    public void run() {

        KafkaConsumer<K, V> consumer = new KafkaConsumer<>(consumerConfig,
                keySerde.deserializer(),
                valueSerde.deserializer());
        consumer.subscribe(Collections.singletonList(topicName));

        this.consumer = Optional.of(consumer);

        try {
            while (!closed.get()) {
                ConsumerRecords<K, V> records = consumer.poll(Duration.ofMillis(100L));
                for (ConsumerRecord<K, V> record : records) {
                    K key = record.key();
                    V value = record.value();
                    processor.accept(key, value);
                }
            }
        } catch (WakeupException e) {
            if (!closed.get()) throw e;
        } finally {
            logger.info("Closing consumer and producer");
            consumer.commitSync();
            consumer.close();
            this.onClose.accept(true);
        }
    }

    public void close() {
        closed.set(true);
        consumer.ifPresent(KafkaConsumer::wakeup);
    }
}
