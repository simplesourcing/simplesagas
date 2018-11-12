package shared.utils

import java.util.Properties

import scala.collection.JavaConverters._
import org.apache.kafka.clients.admin.{AdminClient, CreateTopicsResult, NewTopic}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig, Topology}

final case class StreamAppConfig(appId: String, bootstrapServers: String)

object StreamAppUtils {
  def getConfig(appConfig: StreamAppConfig): Properties = {
    val config: Properties = new Properties
    config.put(StreamsConfig.APPLICATION_ID_CONFIG, appConfig.appId)
    config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, appConfig.bootstrapServers)
    config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    config.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG,
               classOf[LogAndContinueExceptionHandler])
    config.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE)
    config
  }

  def addMissingTopics(adminClient: AdminClient, partitions: Int = 3, replication: Short = 1)(
      topicNames: List[String]): CreateTopicsResult = {
    val newTopics = topicNames.toSet
      .diff(adminClient.listTopics().names().get().asScala)
      .map(topicName => new NewTopic(topicName, partitions, replication))
    adminClient.createTopics(newTopics.asJava)
  }

  def runStreamApp(config: Properties, topology: Topology) = {
    val streams = new KafkaStreams(topology, new StreamsConfig(config))

    streams.cleanUp()
    streams.start()

    Runtime.getRuntime.addShutdownHook(new Thread {
      override def run(): Unit = {
        streams.close()
      }
    })
  }
}
