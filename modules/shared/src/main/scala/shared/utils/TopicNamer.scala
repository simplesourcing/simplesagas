package shared.utils
import io.simplesource.kafka.api.ResourceNamingStrategy
import io.simplesource.kafka.spec.TopicSpec
import io.simplesource.kafka.util.PrefixResourceNamingStrategy
import model.topics.{TopicConfig, TopicNamer}

object TopicNamer {
  def forStrategy(strategy: ResourceNamingStrategy,
                  topicBaseName: String,
                  allTopics: List[String]): TopicNamer =
    topicType => strategy.topicName(topicBaseName, topicType)
  def forPrefix(topicPrefix: String, baseName: String): TopicNamer =
    topicType => new PrefixResourceNamingStrategy(topicPrefix).topicName(baseName, topicType)

}

object TopicConfigurer {
  def forStrategy(strategy: ResourceNamingStrategy,
                  topicBaseName: String,
                  allTopics: List[String]): TopicConfig = {
    val topicNamer: TopicNamer = TopicNamer.forStrategy(strategy, topicBaseName, allTopics)
    new TopicConfig(topicNamer, allTopics, Map.empty)
  }

  final case class TopicCreation(topicName: String, topicSpec: TopicSpec)
  object TopicCreation {
    def apply(topicConfig: TopicConfig)(topicType: String): TopicCreation = {
      val name = topicConfig.namer(topicType)
      val spec = topicConfig.topicSpecs(topicType)
      TopicCreation(name, spec)
    }

    def withCustomName(topicConfig: TopicConfig)(topicType: String, topicName: String): TopicCreation = {
      val spec = topicConfig.topicSpecs(topicType)
      TopicCreation(topicName, spec)
    }
  }

  def getTopics(topicConfig: TopicConfig): List[TopicCreation] = {
    topicConfig.topicSpecs.map {
      case (topicBase, config) => {
        val name = topicConfig.namer(topicBase)
        TopicCreation(name, config)
      }
    }.toList
  }
}
