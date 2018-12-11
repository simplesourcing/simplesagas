package shared
import io.simplesource.saga.shared.topics.{TopicConfigBuilder, TopicNamer}

object TopicUtils {

  def buildSteps(prefix: String, baseName: String): shared.topics.TopicConfigBuilder.BuildSteps =
    builder =>
      builder
        .withTopicNamer(shared.topics.TopicNamer.forPrefix(prefix, baseName))
        .withDefaultConfig(constants.partitions, constants.replication, constants.retentionDays)

  def buildStepsJ(prefix: String, baseName: String): TopicConfigBuilder.BuildSteps =
    builder =>
      builder
        .withTopicNamer(TopicNamer.forPrefix(prefix, baseName))
        .withDefaultConfig(constants.partitions, constants.replication, constants.retentionDays)
}
