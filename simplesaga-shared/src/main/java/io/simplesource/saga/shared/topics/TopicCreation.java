package io.simplesource.saga.shared.topics;

import io.simplesource.kafka.spec.TopicSpec;
import lombok.Value;

import java.util.List;
import java.util.stream.Collectors;

/**
 * A data class that encompasses both the topic name and the configuration for topic creation
 */
@Value(staticConstructor = "of")
public class TopicCreation {
    /**
     * The topic name.
     */
    public final String topicName;
    /**
     * The topic spec for topic creation.
     */
    public final TopicSpec topicSpec;

    /**
     * With custom name topic creation.
     *
     * @param topicConfig the topic config
     * @param topicType   the topic type
     * @param topicName   the topic name
     * @return the topic creation
     */
    public static TopicCreation withCustomName(TopicConfig topicConfig, String topicType, String topicName) {
        TopicSpec spec = topicConfig.topicSpecs.get(topicType);
        return new TopicCreation(topicName, spec);
    }

    /**
     * Creates a list of topic creation items from a topic config, resolving topic names and topic specs
     *
     * @param topicConfig the topic config
     * @return the list
     */
    public static List<TopicCreation> allTopics(TopicConfig topicConfig) {
        return topicConfig.topicSpecs.entrySet().stream().map(kv -> {
            String topicBase = kv.getKey();
            String name = topicConfig.namer.apply(topicBase);
            return new TopicCreation(name, kv.getValue());
        }).collect(Collectors.toList());
    }
}
