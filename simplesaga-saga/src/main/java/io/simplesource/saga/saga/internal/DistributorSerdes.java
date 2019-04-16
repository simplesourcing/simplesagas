package io.simplesource.saga.saga.internal;

import lombok.Value;
import org.apache.kafka.common.serialization.Serde;

@Value
final class DistributorSerdes<K, V> {
    public final Serde<K> key;
    public final Serde<V> value;
}
