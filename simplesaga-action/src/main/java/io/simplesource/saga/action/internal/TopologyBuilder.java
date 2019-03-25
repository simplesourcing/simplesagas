package io.simplesource.saga.action.internal;

import io.simplesource.saga.shared.utils.StreamAppConfig;
import org.apache.kafka.streams.Topology;

/**
 * Kafka Streams topology builder.
 */
public interface TopologyBuilder {

    /**
     * Build the Kafka Streams topology.
     * @param config Kafka configuration.
     * @return topology.
     */
    Topology build(StreamAppConfig config);
}
