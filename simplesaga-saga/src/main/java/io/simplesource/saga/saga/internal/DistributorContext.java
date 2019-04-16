package io.simplesource.saga.saga.internal;

import io.simplesource.kafka.spec.WindowSpec;
import lombok.Value;

import java.util.UUID;
import java.util.function.Function;

@Value
final class DistributorContext<K, V> {
    public final DistributorSerdes<K, V> serdes;
    public final String topicNameMapTopic;
    public final WindowSpec responseWindowSpec;
    public final Function<V, K> idMapper;
    public final Function<K, UUID> keyToUuid;
}
