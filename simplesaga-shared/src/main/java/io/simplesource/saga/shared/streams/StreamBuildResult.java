package io.simplesource.saga.shared.streams;

import io.simplesource.saga.shared.topics.TopicCreation;
import lombok.Value;
import org.apache.kafka.streams.Topology;

import java.util.List;
import java.util.function.Supplier;

@Value
public class StreamBuildResult {
    public final List<TopicCreation> topicCreations;
    public final Supplier<Topology> topologySupplier;
    public final List<StreamAppUtils.ShutdownHandler> shutdownHandlers;
}
