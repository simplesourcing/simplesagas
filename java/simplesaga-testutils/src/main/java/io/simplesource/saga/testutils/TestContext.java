package io.simplesource.saga.testutils;

import lombok.Value;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.TopologyTestDriver;

@Value
public class TestContext {
    private final TopologyTestDriver testDriver;

    public <K, V> RecordPublisher<K, V> publisher(String topicName, Serde<K> keySerde, Serde<V> valueSerde) {
        return new TestRecordPublisher<>(testDriver, keySerde, valueSerde, topicName);
    }

    public <K, V> RecordVerifier<K, V> verifier(String topicName, Serde<K> keySerde, Serde<V> valueSerde) {
        return new RecordVerifier<>(testDriver, keySerde, valueSerde, topicName);
    }
}
