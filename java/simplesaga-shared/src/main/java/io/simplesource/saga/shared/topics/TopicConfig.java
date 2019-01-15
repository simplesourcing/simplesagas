package io.simplesource.saga.shared.topics;
import io.simplesource.kafka.spec.TopicSpec;
import lombok.Value;

import java.util.List;
import java.util.Map;


@Value
public final class TopicConfig {
    public TopicNamer namer;
    public List<String> topicTypes;
    public Map<String, TopicSpec> topicSpecs;
}
