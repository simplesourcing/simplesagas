package io.simplesource.saga.shared.streams;

import io.simplesource.saga.shared.topics.TopicCreation;
import lombok.Value;
import org.apache.kafka.streams.StreamsBuilder;

import java.util.List;
import java.util.function.Function;

@Value
public class StreamBuildSpec {
    public final List<TopicCreation> topics;
    public final Function<StreamsBuilder, StreamAppUtils.ShutdownHandler> topologyBuildStep;
}
