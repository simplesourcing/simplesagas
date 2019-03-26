package io.simplesource.saga.action.common;

import io.simplesource.saga.shared.topics.TopicCreation;
import io.simplesource.saga.shared.utils.StreamAppUtils;
import lombok.Value;
import org.apache.kafka.streams.StreamsBuilder;

import java.util.List;
import java.util.Optional;
import java.util.function.Function;

@Value
public class StreamBuildSpec {
    public final List<TopicCreation> topics;
    public final Function<StreamsBuilder, StreamAppUtils.ShutdownHandler> topologyBuildStep;
}
