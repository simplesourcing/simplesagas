package io.simplesource.shared.topics;
import io.simplesource.kafka.spec.TopicSpec;
import lombok.Value;

import java.util.List;
import java.util.Map;


@Value
final class TopicConfig {
    public TopicNamer namer;
    public List<String> topicSpecs;
    public Map<String, TopicSpec> topicSpecs;
}
