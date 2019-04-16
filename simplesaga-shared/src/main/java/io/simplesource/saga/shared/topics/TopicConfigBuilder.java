package io.simplesource.saga.shared.topics;

import io.simplesource.kafka.api.ResourceNamingStrategy;
import io.simplesource.kafka.spec.TopicSpec;
import io.simplesource.kafka.util.PrefixResourceNamingStrategy;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static org.apache.kafka.common.config.TopicConfig.*;

/***
 * TopicConfigBuilder, as used with {@link BuildSteps TopicConfigBuilder.BuildSteps} is a functional pattern for setting topic configuration
 *
 * Whenever a {@link BuildSteps TopicConfigBuilder.BuildSteps} is required, a user can provide a lambda function of this form for example:
 * <pre>{@code
 * builder -> builder
 *     .withDefaultTopicSpec(partitions, replication, retentionInDays)
 *     .with
 *     .withTopicPrefix("prefix")
 * }*</pre>
 *
 * This pattern allows the framework flexibility how it treats this configuration, without complicating the user interaction
 * as the framework determines when the config is built.
 * <p>
 * The topic configuration covers both the topic naming conventions, as well as the configuration required to create the missing topics.
 * <p>
 * This pattern enables topics names and configurations to have sensible out the box defaults,
 * so minimal up-front configuration is required, but still allow for complete flexibility.
 */
public class TopicConfigBuilder {

    /**
     * A functional interface representing a configuration step that is applied to a topic builder
     */
    @FunctionalInterface
    public interface BuildSteps {
        TopicConfigBuilder applyStep(TopicConfigBuilder builder);

        default BuildSteps withInitialStep(BuildSteps initial) {
            return builder -> this.applyStep(initial.applyStep(builder));
        }

        default BuildSteps withNextStep(BuildSteps initial) {
            return builder -> initial.applyStep(this.applyStep(builder));
        }

        default TopicConfig build(List<String> topicTypes) {
            return this.build(topicTypes, Collections.emptyMap(), Collections.emptyMap());
        }

        default TopicConfig build(List<String> topicTypes,
                                  Map<String, String> defaultConfigs,
                                  Map<String, Map<String, String>> defaultOverrides) {
            TopicConfigBuilder topicBuilder = new TopicConfigBuilder(topicTypes, defaultConfigs, defaultOverrides);
            this.applyStep(topicBuilder);
            return topicBuilder.buildTopicConfig();
        }
    }

    private final List<String> topicTypes;
    private final Map<String, String> defaultConfigs;
    private ResourceNamingStrategy namingStrategy = null;
    private String topicBaseName = null;
    private TopicNamer topicNamer = null;

    private final Map<String, Map<String, String>> defaultOverrides;
    private final Map<String, TopicSpec> topicSpecOverrides = new HashMap<>();
    private final Map<String, String> topicNameOverrides = new HashMap<>();

    private Function<String, TopicSpec> defaultTopicSpec = topicType ->
            topicSpecForParallelismAndRetention(1, 1, 7, topicType);

    private TopicConfigBuilder(
            List<String> topicTypes,
            Map<String, String> defaultConfigs,
            Map<String, Map<String, String>> defaultOverrides) {
        this.topicTypes = topicTypes;
        this.defaultConfigs = defaultConfigs;
        this.defaultOverrides = defaultOverrides;
    }

    /**
     * Sets the the resource naming strategy for topic creation
     *
     * @param namingStrategy the naming strategy
     * @return the topic config builder
     */
    public TopicConfigBuilder withNamingStrategy(ResourceNamingStrategy namingStrategy) {
        this.namingStrategy = namingStrategy;
        return this;
    }

    /**
     * Sets the resource naming strategy to {@code PrefixResourceNamingStrategy} with the given prefix
     *
     * @param prefix the prefix
     * @return the topic config builder
     */
    public TopicConfigBuilder withTopicPrefix(String prefix) {
        this.namingStrategy = new PrefixResourceNamingStrategy(prefix);
        return this;
    }

    /**
     * Sets the base name for topic creation when applying the prefix resource naming strategy
     *
     * @param topicBaseName the topic base name
     * @return the topic config builder
     */
    public TopicConfigBuilder withTopicBaseName(String topicBaseName) {
        this.topicBaseName = topicBaseName;
        return this;
    }

    /**
     * Explicitly sets the topic namer
     *
     * @param topicNamer the topic namer
     * @return the topic config builder
     */
    public TopicConfigBuilder withTopicNamer(TopicNamer topicNamer) {
        this.topicNamer = topicNamer;
        return this;
    }

    /**
     * Overrides the topic name for a given topic type
     *
     * @param topicType the topic type
     * @param topicName the topic name
     * @return the topic config builder
     */
    public TopicConfigBuilder withTopicNameOverride(String topicType, String topicName) {
        topicNameOverrides.put(topicType, topicName);
        return this;
    }

    /**
     * Overrides the topic spec for a given topic type
     *
     * @param topicType the topic type
     * @param topicSpec     the topic spec
     * @return the topic config builder
     */
    public TopicConfigBuilder withTopicSpecOverride(String topicType, TopicSpec topicSpec) {
        topicSpecOverrides.put(topicType, topicSpec);
        return this;
    }

    /**
     * Sets the topic spec by default topic to be the topic spec with the specified partitions and replication factor, as well as the setting the topic retention
     *
     * @param partitions      the partitions
     * @param replication     the replication
     * @param retentionInDays the retention in days
     * @return the topic config builder
     */
    public TopicConfigBuilder withDefaultTopicSpec(int partitions, int replication, int retentionInDays) {
        defaultTopicSpec = topicType -> topicSpecForParallelismAndRetention(partitions, replication, retentionInDays, topicType);
        return this;
    }

    private TopicConfig buildTopicConfig() {
        TopicNamer namer = getTopicNamer();
        Map<String, TopicSpec> topicSpecs = new HashMap<>();
        topicTypes.forEach(tt ->
                topicSpecs.put(tt, topicSpecOverrides.getOrDefault(tt, defaultTopicSpec.apply(tt))));
        return new TopicConfig(namer, topicTypes, topicSpecs);
    }

    private TopicNamer baseTopicNamer() {
        if (topicNamer != null) return topicNamer;
        if (topicBaseName != null) return TopicNamer.forStrategy(
                namingStrategy != null ? namingStrategy : new PrefixResourceNamingStrategy(),
                topicBaseName);
        return name -> name;
    }

    private TopicNamer getTopicNamer() {
        TopicNamer baseNamer = baseTopicNamer();
        return name -> {
            String override = topicNameOverrides.get(name);
            if (override != null) return override;
            return baseNamer.apply(name);
        };
    }

    private TopicSpec topicSpecForParallelismAndRetention(int partitions, int replication, long retentionInDays, String topicType) {
        Map<String, String> configMap = defaultOverrides.getOrDefault(topicType, defaultConfigs);
        // configure retention if it is not already set

        String retentionString = String.valueOf(TimeUnit.DAYS.toMillis(retentionInDays));
        HashMap<String, String> dMap = new HashMap<>();
        if (configMap.getOrDefault(CLEANUP_POLICY_CONFIG, "").equals(CLEANUP_POLICY_COMPACT) &&
                !configMap.getOrDefault(DELETE_RETENTION_MS_CONFIG, "").equals("")) {
            dMap.put(DELETE_RETENTION_MS_CONFIG, retentionString);
            dMap.put(MIN_COMPACTION_LAG_MS_CONFIG, retentionString);
        } else {
            dMap.put(RETENTION_MS_CONFIG, retentionString);
        }

        Map<String, String> usedMap = defaultOverrides.getOrDefault(topicType, defaultConfigs);
        usedMap.forEach(dMap::put);
        return new TopicSpec(partitions, (short) replication, dMap);
    }
}
