package shared.utils
import java.util.concurrent.TimeUnit

import io.simplesource.kafka.spec.TopicSpec
import org.apache.kafka.common.config.{TopicConfig => KafkaTopicConfig}
import model.topics.{TopicConfig, TopicNamer}
import scala.collection.JavaConverters._

final case class TopicConfigBuilder(topicTypes: List[String],
                                    defaultConfigs: Map[String, String],
                                    defaultOverrides: Map[String, Map[String, String]] = Map.empty) {
  val configMap                      = new scala.collection.mutable.HashMap[String, TopicSpec]()
  var default: String => TopicSpec   = defaultMap(1, 1, 7)
  var topicNamer: Option[TopicNamer] = None

  def withTopicNamer(topicNamer: TopicNamer): TopicConfigBuilder = {
    this.topicNamer = Some(topicNamer)
    this
  }

  def withDefaultConfig(partitions: Int, replication: Int, retentionInDays: Long): TopicConfigBuilder = {
    default = defaultMap(partitions, replication, retentionInDays)
    this
  }

  def withConfig(topicType: String, tSpec: TopicSpec): TopicConfigBuilder = {
    configMap.put(topicType, tSpec)
    this
  }

  private def defaultMap(partitions: Int, replication: Int, retentionInDays: Long)(
      topicType: String): TopicSpec = {
    val retMap = Map(
      KafkaTopicConfig.RETENTION_MS_CONFIG -> String.valueOf(TimeUnit.DAYS.toMillis(retentionInDays)))
    val usedMap = defaultOverrides.getOrElse(topicType, defaultConfigs) ++ retMap
    new TopicSpec(partitions, replication.toShort, usedMap.asJava)
  }

  def build(): TopicConfig = {
    val topicSpecs = topicTypes.map { tt =>
      (tt, configMap.getOrElse(tt, default(tt)))
    }.toMap
    // TODO check topicNamer is defined
    TopicConfig(topicNamer.get, topicTypes, topicSpecs)
  }
}
