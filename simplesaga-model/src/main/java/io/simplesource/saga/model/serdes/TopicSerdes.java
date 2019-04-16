package io.simplesource.saga.model.serdes;

import lombok.Value;
import org.apache.kafka.common.serialization.Serde;

/**
 * A generic interface representing the serdes required to serialize and deserialize an arbitrary topic.
 *
 * @param <K> the key type parameter
 * @param <V> the value type parameter
 */
@Value
public final class TopicSerdes<K, V>{
    /**
     * Serde for the key
     */
    public final Serde<K> key;
    /**
     * Serde for the value
     */
    public final Serde<V> value;
}
