package io.simplesource.saga.testutils;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.TopologyTestDriver;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;

import static org.assertj.core.api.Assertions.assertThat;

public class RecordVerifier<K, V> {
    private final TopologyTestDriver driver;
    private final Serde<K> keySerde;
    private final Serde<V> valueSerde;
    private final String topicName;

    RecordVerifier(final TopologyTestDriver driver, final Serde<K> keySerde, final Serde<V> valueSerde, String topicName) {
        this.driver = driver;
        this.keySerde = keySerde;
        this.valueSerde = valueSerde;
        this.topicName = topicName;
    }

    public V verifyAndReturn(BiConsumer<K, V> verifier) {
        ProducerRecord<K, V> record = driver.readOutput(topicName,
                keySerde.deserializer(),
                valueSerde.deserializer());
        if (record == null)
            return null;
        if (verifier != null) {
            verifier.accept(record.key(), record.value());
        }
        return record.value();
    }

    public void verifyNoRecords() {
        V x = verifyAndReturn(null);
        assertThat(x).isNull();

    }

    public V verifySingle(BiConsumer<K, V> verifier) {
        return verifyAndReturn(verifier::accept);
    }

    public List<V> verifyMultiple(int count, TriConsumer<Integer, K, V> verifier) {
        List<V> eventList = new ArrayList<>();
        int[] index = new int[1];
        while (true) {
            V response = verifyAndReturn((k, v) -> verifier.accept(index[0], k, v));
            if (response == null) break;
            index[0] = index[0] + 1;
            eventList.add(response);
        }
        if (count >= 0)
            assertThat(eventList).hasSize(count);
        return eventList;
    }

    public void drainAll() {
        verifyMultiple(-1, (i, k, v) -> {});
    }
}
