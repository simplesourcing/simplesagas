package io.simplesource.saga.testutils;

import io.simplesource.saga.shared.topics.TopicConfigBuilder;
import io.simplesource.saga.shared.topics.TopicNamer;

public class TopicUtils {
    public static TopicConfigBuilder.BuildSteps buildSteps(String prefix, String baseName) {
        return builder -> builder.withTopicNamer(TopicNamer.forPrefix(prefix, baseName))
                .withDefaultConfig(Constants.PARTITIONS, Constants.REPLICATION, Constants.RETENTION_DAYS);
    }
}
