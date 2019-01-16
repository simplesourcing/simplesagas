package io.simplesource.saga.shared.topics;

import io.simplesource.kafka.spec.TopicSpec;
import lombok.Value;

import java.util.List;
import java.util.stream.Collectors;

@Value
public class TopicCreation {
    public String topicName;
    public TopicSpec topicSpec;

    public static TopicCreation apply(TopicConfig topicConfig, String topicType) {
        String name = topicConfig.namer.apply(topicType);
        TopicSpec spec = topicConfig.topicSpecs.get(topicType);
        return new TopicCreation(name, spec);
    }

    public static TopicCreation withCustomName(TopicConfig topicConfig, String topicType, String topicName) {
        TopicSpec spec = topicConfig.topicSpecs.get(topicType);
        return new TopicCreation(topicName, spec);
    }

    public static List<TopicCreation> allTopics(TopicConfig topicConfig) {
        return topicConfig.topicSpecs.entrySet().stream().map(kv -> {
            String topicBase = kv.getKey();
            String name = topicConfig.namer.apply(topicBase);
            return new TopicCreation(name, kv.getValue());
        }).collect(Collectors.toList());
    }
}
