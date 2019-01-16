package io.simplesource.saga.shared.topics;

import io.simplesource.kafka.api.ResourceNamingStrategy;
import io.simplesource.kafka.util.PrefixResourceNamingStrategy;

import java.util.List;

public interface TopicNamer {
    String apply(String topicType);

    static TopicNamer forStrategy(ResourceNamingStrategy strategy,
                    String topicBaseName) {
        return topicType -> strategy.topicName(topicBaseName, topicType);
    }

    static TopicNamer forPrefix(String topicPrefix, String baseName) {
        return topicType -> new PrefixResourceNamingStrategy(topicPrefix).topicName(baseName, topicType);
    }
}
