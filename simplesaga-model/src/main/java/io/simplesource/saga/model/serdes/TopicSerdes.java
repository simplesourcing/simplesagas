package io.simplesource.saga.model.serdes;

import lombok.Value;
import org.apache.kafka.common.serialization.Serde;

/**
 * The type Topic serdes.
 *
 * @param <K> the type parameter
 * @param <V> the type parameter
 */
@Value
public final class TopicSerdes<K, V>{
    /**
     * The Key.
     */
    public final Serde<K> key;
    /**
     * The Value.
     */
    public final Serde<V> value;
}
