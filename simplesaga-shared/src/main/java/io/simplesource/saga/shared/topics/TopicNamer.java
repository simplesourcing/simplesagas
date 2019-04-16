package io.simplesource.saga.shared.topics;

import io.simplesource.kafka.api.ResourceNamingStrategy;
import io.simplesource.kafka.util.PrefixResourceNamingStrategy;

/**
 * Several types of topics (eg. request, response) need to be created for any given action type, and for the sagas themselves.
 * <p>
 * TopicNamer ensures a consistent naming convention for these families of topics.
 */
@FunctionalInterface
public interface TopicNamer {
    /**
     * Create a full topic name for a topic type
     *
     * @param topicType the topic type
     * @return the topic name
     */
    String apply(String topicType);

    /**
     * Creates a topic namer using a resource naming strategy.
     *
     * @param strategy      the strategy
     * @param topicBaseName the topic base name
     * @return the topic namer
     */
    static TopicNamer forStrategy(ResourceNamingStrategy strategy, String topicBaseName) {
        return topicType -> strategy.topicName(topicBaseName, topicType);
    }

    /**
     * Creates a topic namer using the prefix resource naming strategy.
     *
     * Ths creates a topic namer equivalent to the function
     * <pre>{@code
     * topicType -> topicPrefix + "-" + baseName + "-" + topicType
     * }</pre>
     *
     * @param topicPrefix the topic prefix
     * @param baseName    the base name
     * @return the topic namer
     */
    static TopicNamer forPrefix(String topicPrefix, String baseName) {
        return forStrategy(new PrefixResourceNamingStrategy(topicPrefix), baseName);
    }
}
