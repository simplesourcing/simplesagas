package io.simplesource.saga.testutils;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.StreamsBuilder;

public final class StreamContext {
    private final StreamsBuilder builder;

    public StreamContext(StreamsBuilder builder) {
        this.builder = builder;
    }

    public <K> Serde<K> getKeySerde() {
        return null;
    }

    public <V> Serde<V> getValueSerde() {
        return null;
    }

    public StreamsBuilder getBuilder() {
        return builder;
    }
}
