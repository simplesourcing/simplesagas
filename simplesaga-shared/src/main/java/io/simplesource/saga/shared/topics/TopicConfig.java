package io.simplesource.saga.shared.topics;

import io.simplesource.kafka.spec.TopicSpec;
import lombok.Value;

import java.util.List;
import java.util.Map;

@Value
public final class TopicConfig {
    public final TopicNamer namer;
    public final List<String> topicTypes;
    public final Map<String, TopicSpec> topicSpecs;
}
