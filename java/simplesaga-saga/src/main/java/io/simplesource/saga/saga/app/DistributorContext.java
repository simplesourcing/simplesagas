package io.simplesource.saga.saga.app;

import io.simplesource.kafka.spec.WindowSpec;
import lombok.Value;

import java.util.UUID;
import java.util.function.Function;

@Value
public final class DistributorContext<V> {
    public final DistributorSerdes<V> serdes;
    public final String topicNameMapTopic;
    public final WindowSpec responseWindowSpec;
    public final Function<V, UUID> idMapper;

    public DistributorContext(DistributorSerdes<V> serdes,
                              String topicNameMapTopic,
                              WindowSpec responseWindowSpec,
                              Function<V, UUID> idMapper) {
        this.serdes = serdes;
        this.topicNameMapTopic = topicNameMapTopic;
        this.responseWindowSpec = responseWindowSpec;
        this.idMapper = idMapper;
    }
}
