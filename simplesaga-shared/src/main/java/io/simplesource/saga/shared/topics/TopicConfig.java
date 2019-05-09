package io.simplesource.saga.shared.topics;

import io.simplesource.kafka.spec.TopicSpec;
import lombok.Value;

import java.util.List;
import java.util.Map;

/**
 * TopicConfig represents configuration information for topic names and for topic properties for topic creation
 * <p>
 * It ensures that the topic names for a family of topics are consistent
 * <p>
 * Examples of families of topics are:
 * <ul>
 *     <li>Action topics for a given action type (request, response etc)</li>
 *     <li>Saga topics (request, response as well as saga state and transitions)</li>
 * </ul>
 * See {@link TopicTypes} for complete lists of topic families.
 */
@Value
public final class TopicConfig {
    /**
     * A functional interface that resolves the names of topics for a set of topics types.
     */
    public final TopicNamer namer;
    /**
     * A list of topic types that this topic config applies to the family of topics
     */
    public final List<String> topicTypes;
    /**
     * A map of topic specs per action type. The topic spec defines the properties required for topic creation
     */
    public final Map<String, TopicSpec> topicSpecs;

    /**
     * Builds a list of topic creations, which contains both the topic namer and the topic spec
     *
     * @return the list of topic creations
     */
    public final List<TopicCreation> allTopics() { return TopicCreation.allTopics(this); }
}
