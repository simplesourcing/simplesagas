package io.simplesource.saga.shared.topics;

import io.simplesource.kafka.spec.TopicSpec;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static org.apache.kafka.common.config.TopicConfig.*;

public class TopicConfigBuilder {
    private final List<String> topicTypes;
    private final Map<String, String> defaultConfigs;
    private final Map<String, Map<String, String>> defaultOverrides;

    private Map<String, TopicSpec> configMap = new HashMap<String, TopicSpec>();
    private Function<String, TopicSpec> defaultSpec = topicType -> defaultMap(1, 1, 7, topicType);
    private TopicNamer topicNamer = name -> name;

    @FunctionalInterface
    public interface BuildSteps {
        TopicConfigBuilder applyStep(TopicConfigBuilder builder);
    }

    public TopicConfigBuilder(
            List<String> topicTypes,
            Map<String, String> defaultConfigs,
            Map<String, Map<String, String>> defaultOverrides) {
        this.topicTypes = topicTypes;
        this.defaultConfigs = defaultConfigs;
        this.defaultOverrides = defaultOverrides;
    }

    public TopicConfigBuilder  withTopicNamer(TopicNamer topicNamer) {
        this.topicNamer = topicNamer;
        return this;
    }

    public TopicConfigBuilder withConfig(String topicType, TopicSpec tSpec) {
        configMap.put(topicType, tSpec);
        return this;
    }

    public TopicConfigBuilder withDefaultConfig(int partitions, int replication, int retentionInDays) {
        defaultSpec = topicType -> defaultMap(partitions, replication, retentionInDays, topicType);
        return this;
    }

    public TopicConfig build() {
        Map<String, TopicSpec> topicSpecs = new HashMap<>();
        topicTypes.forEach(tt ->
                topicSpecs.put(tt, configMap.getOrDefault(tt, defaultSpec.apply(tt))));
        return new TopicConfig(topicNamer, topicTypes, topicSpecs);
    }

    public TopicSpec defaultMap(int partitions, int replication, long retentionInDays, String topicType) {
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

    public static TopicConfig buildTopics(List<String> topicTypes,
                                          BuildSteps buildSteps) {
        TopicConfigBuilder topicBuilder = new TopicConfigBuilder(topicTypes, Collections.emptyMap(), Collections.emptyMap());
        buildSteps.applyStep(topicBuilder);
        return topicBuilder.build();
    }

    public static TopicConfig buildTopics(List<String> topicTypes,
                                          Map<String, String> defaultConfigs,
                                          Map<String, Map<String, String>> defaultOverrides,
                                          BuildSteps buildSteps) {
        TopicConfigBuilder topicBuilder = new TopicConfigBuilder(topicTypes, defaultConfigs, defaultOverrides);
        buildSteps.applyStep(topicBuilder);
        return topicBuilder.build();
    }

}
