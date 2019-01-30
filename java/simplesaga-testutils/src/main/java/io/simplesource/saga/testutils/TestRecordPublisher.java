package io.simplesource.saga.testutils;

import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.test.ConsumerRecordFactory;

public class TestRecordPublisher<K extends SpecificRecord, V extends SpecificRecord> implements RecordPublisher<K, V> {
    private final ConsumerRecordFactory<K, V> factory;
    private final TopologyTestDriver driver;
    private final String topicName;

    TestRecordPublisher(final TopologyTestDriver driver, StreamContext context, String topicName) {
        final Serde<K> keySerde = context.getKeySerde();
        final Serde<V> valueSerde = context.getValueSerde();
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

