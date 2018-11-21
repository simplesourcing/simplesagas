package action.sourcing
import java.util.concurrent.TimeUnit
import java.util.{Properties, UUID}

import action.common.ActionConsumer
import io.simplesource.kafka.spec.TopicSpec
import model.{messages, topics => topicNames}
import model.messages.{ActionRequest, ActionResponse}
import model.serdes.ActionSerdes
import model.specs.ActionProcessorSpec
import model.topics.{TopicConfig, TopicNamer}
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.common.config.{TopicConfig => KafkaTopicConfig}
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.kstream.KStream
import org.slf4j.LoggerFactory
import shared.utils.TopicConfigurer.TopicCreation
import shared.utils.{StreamAppConfig, StreamAppUtils, TopicConfigBuilder}

import scala.collection.JavaConverters._

final case class SourcingApp[A](actionSerdes: ActionSerdes[A],
                                topicBuildFn: TopicConfigBuilder => TopicConfigBuilder) {
  private val actionTopicConfig = {
    val topicConfigBuilder = TopicConfigBuilder(topicNames.ActionTopic.all, Map.empty)
    topicBuildFn(topicConfigBuilder).build()
  }
  private val actionSpec = ActionProcessorSpec[A](actionSerdes, actionTopicConfig)
  private val logger     = LoggerFactory.getLogger(classOf[SourcingApp[A]])

  final case class CommandInput(builder: StreamsBuilder,
                                actionRequests: KStream[UUID, ActionRequest[A]],
                                actionResponses: KStream[UUID, ActionResponse])

  type Command = CommandInput => Unit

  private var commands: List[Command]     = List.empty
  private var topics: List[TopicCreation] = topicNames.ActionTopic.all.map(TopicCreation(actionTopicConfig))

  def addCommand[I, K, C](cSpec: CommandSpec[A, I, K, C],
                          topicBuildFn: TopicConfigBuilder => TopicConfigBuilder): SourcingApp[A] = {
    val commandTopicConfig = {
      val topicConfigBuilder = TopicConfigBuilder(topicNames.CommandTopic.all, Map.empty)
      topicBuildFn(topicConfigBuilder).build()
    }
    val actionContext = SourcingContext(actionSpec, cSpec, commandTopicConfig.namer)
    val command: Command = input => {
      val commandResponses =
        CommandConsumer.commandResponseStream(cSpec, commandTopicConfig.namer, input.builder)
      SourcingStream.addSubTopology(actionContext,
                                    input.actionRequests,
                                    input.actionResponses,
                                    commandResponses)
    }
    commands = command :: commands
    topics = topics ++ topicNames.CommandTopic.all.map(TopicCreation(commandTopicConfig))
    this
  }

  def run(appConfig: StreamAppConfig): Unit = {
    val config: Properties = StreamAppUtils.getConfig(appConfig)

    StreamAppUtils
      .addMissingTopics(AdminClient.create(config))(topics.distinct)
      .all()
      .get(30L, TimeUnit.SECONDS)

    val builder = new StreamsBuilder()
    val actionRequests: KStream[UUID, messages.ActionRequest[A]] =
      ActionConsumer.actionRequestStream(actionSpec, builder)
    val actionResponses: KStream[UUID, messages.ActionResponse] =
      ActionConsumer.actionResponseStream(actionSpec, builder)

    val commandInput = CommandInput(builder, actionRequests, actionResponses)
    commands.foreach(_(commandInput))
    val topology = builder.build()
    logger.info("Topology description {}", topology.describe())
    StreamAppUtils.runStreamApp(config, topology)
  }
}
