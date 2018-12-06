package io.simplesource.saga.saga.app;

import io.simplesource.kafka.spec.WindowSpec;
import lombok.Value;
import org.apache.kafka.common.serialization.Serde;

import java.util.UUID;
import java.util.function.Function;

@Value
public class DistributorContext<V> {
    @Value
    public static class DistributorSerdes<V>  {
        public final Serde<UUID> uuid;
        public final Serde<V> value;
    }

    public final String topicNameMapTopic;
    public final DistributorSerdes<V> serdes;
    public final WindowSpec responseWindowSpec;
    public final Function<V, UUID> idMapper;
}
