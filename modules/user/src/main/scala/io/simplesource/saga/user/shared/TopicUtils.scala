package io.simplesource.saga.user.shared
import io.simplesource.saga.shared.topics.{TopicConfigBuilder, TopicNamer}

object TopicUtils {

  def buildSteps(prefix: String, baseName: String): TopicConfigBuilder.BuildSteps =
    builder =>
      builder
        .withTopicNamer(TopicNamer.forPrefix(prefix, baseName))
        .withDefaultConfig(constants.partitions, constants.replication, constants.retentionDays)
}
