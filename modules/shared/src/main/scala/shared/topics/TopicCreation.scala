package shared.topics

import io.simplesource.kafka.spec.TopicSpec
import shared.topics

final case class TopicCreation(topicName: String, topicSpec: TopicSpec)

object TopicCreation {
  def apply(topicConfig: TopicConfig)(topicType: String): TopicCreation = {
    val name = topicConfig.namer(topicType)
    val spec = topicConfig.topicSpecs(topicType)
    topics.TopicCreation(name, spec)
  }

  def withCustomName(topicConfig: TopicConfig, topicType: String)(topicName: String): TopicCreation = {
    val spec = topicConfig.topicSpecs(topicType)
    topics.TopicCreation(topicName, spec)
  }

  def allTopics(topicConfig: TopicConfig): List[TopicCreation] = {
    topicConfig.topicSpecs.map {
      case (topicBase, config) => {
        val name = topicConfig.namer(topicBase)
        topics.TopicCreation(name, config)
      }
    }.toList
  }
}
