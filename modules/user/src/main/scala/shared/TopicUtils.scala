package shared
import shared.topics.TopicConfigBuilder
object TopicUtils {

  def buildSteps(prefix: String, baseName: String): TopicConfigBuilder.BuildSteps =
    builder =>
      builder
        .withTopicNamer(TopicNamer.forPrefix(prefix, baseName))
        .withDefaultConfig(constants.partitions, constants.replication, constants.retentionDays)
}
