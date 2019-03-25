package io.simplesource.saga.testutils;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.test.ConsumerRecordFactory;

public class TestRecordPublisher<K, V> implements RecordPublisher<K, V> {
    private final ConsumerRecordFactory<K, V> factory;
    private final TopologyTestDriver driver;
    private final String topicName;

    TestRecordPublisher(final TopologyTestDriver driver, Serde<K> keySerde, Serde<V> valueSerde, String topicName) {
        this.driver = driver;
        this.topicName = topicName;
        factory = new ConsumerRecordFactory<>(keySerde.serializer(), valueSerde.serializer());
    }

    private ConsumerRecordFactory<K, V> recordFactory() {
        return factory;
    }

    @Override
    public void publish(final K key, V value) {
        driver.pipeInput(recordFactory().create(topicName, key, value));
    }
}

