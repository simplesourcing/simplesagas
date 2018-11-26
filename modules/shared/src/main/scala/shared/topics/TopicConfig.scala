package shared.topics
import io.simplesource.kafka.spec.TopicSpec
final case class TopicConfig(namer: TopicNamer, topicTypes: List[String], topicSpecs: Map[String, TopicSpec])
