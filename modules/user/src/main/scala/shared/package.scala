import shared.topics.{TopicConfigBuilder, TopicNamer}
package object shared {

  def buildSteps(prefix: String, baseName: String): TopicConfigBuilder.BuildSteps =
    builder =>
      builder
        .withTopicNamer(TopicNamer.forPrefix(prefix, baseName))
        .withDefaultConfig(constants.partitions, constants.replication, constants.retentionDays)
}
