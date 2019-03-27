package io.simplesource.saga.model.serdes;

import lombok.Value;
import org.apache.kafka.common.serialization.Serde;

@Value
public final class TopicSerdes<K, V>{
    public final Serde<K> key;
    public final Serde<V> value;
}
