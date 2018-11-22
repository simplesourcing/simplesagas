package shared.topics

import io.simplesource.kafka.api.ResourceNamingStrategy
import io.simplesource.kafka.util.PrefixResourceNamingStrategy

trait TopicNamer {
  def apply(topicType: String): String
}

object TopicNamer {
  def forStrategy(strategy: ResourceNamingStrategy,
                  topicBaseName: String,
                  allTopics: List[String]): TopicNamer =
    topicType => strategy.topicName(topicBaseName, topicType)

  def forPrefix(topicPrefix: String, baseName: String): TopicNamer =
    topicType => new PrefixResourceNamingStrategy(topicPrefix).topicName(baseName, topicType)
}
