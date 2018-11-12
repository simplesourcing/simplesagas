package shared.utils
import io.simplesource.kafka.api.ResourceNamingStrategy
import model.topics.TopicNamer

object TopicNamer {
  def forStrategy(strategy: ResourceNamingStrategy,
                  topicBaseName: String,
                  allTopics: List[String]): TopicNamer[String] =
    new TopicNamer[String] {
      override def apply(topicType: String): String = strategy.topicName(topicBaseName, topicType)
      override def all(): List[String]              = allTopics.map(a => apply(a))
    }
}
