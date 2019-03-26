package io.simplesource.saga.shared.topics;

import io.simplesource.kafka.api.ResourceNamingStrategy;
import io.simplesource.kafka.util.PrefixResourceNamingStrategy;

/**
 * Several types of topics (eg. request, response) need to be created for any given base/aggregate.
 * TopicNamer ensures a consistent naming convention for each base/aggregate.
 */
public interface TopicNamer {
    String apply(String topicType);

    static TopicNamer forStrategy(ResourceNamingStrategy strategy, String topicBaseName) {
        return topicType -> strategy.topicName(topicBaseName, topicType);
    }

    static TopicNamer forPrefix(String topicPrefix, String baseName) {
        return forStrategy(new PrefixResourceNamingStrategy(topicPrefix), baseName);
    }
}
