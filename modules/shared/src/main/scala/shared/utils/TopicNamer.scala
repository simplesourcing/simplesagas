package shared.utils
import io.simplesource.kafka.api.ResourceNamingStrategy
import model.topics.TopicNamer

object TopicNamer {
  def forStrategy(strategy: ResourceNamingStrategy,
                  topicBaseName: String,
                  allTopics: List[String]): TopicNamer =
    topicType => strategy.topicName(topicBaseName, topicType)
}
