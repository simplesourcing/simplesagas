package io.simplesource.saga.saga.app;

import lombok.Value;
import org.apache.kafka.common.serialization.Serde;

@Value
public final class DistributorSerdes<K, V> {
    public final Serde<K> key;
    public final Serde<V> value;
}
