package action.sourcing
import java.util.concurrent.TimeUnit
import java.util.{Properties, UUID}

import action.common.ActionConsumer
import model.messages
import model.messages.{ActionRequest, ActionResponse}
import model.specs.ActionProcessorSpec
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.kstream.KStream
import org.slf4j.LoggerFactory
import shared.utils.{StreamAppConfig, StreamAppUtils}

final case class SourcingApp[A](actionSpec: ActionProcessorSpec[A]) {
  private val logger = LoggerFactory.getLogger("SourcingApp")

  final case class CommandInput(builder: StreamsBuilder,
                                actionRequests: KStream[UUID, ActionRequest[A]],
                                actionResponses: KStream[UUID, ActionResponse])

  type Command = CommandInput => Unit

  private var commands: List[Command] = List.empty
  private var topics: List[String]    = actionSpec.topicNamer.all()

  def addCommand[I, K, C](cSpec: CommandSpec[A, I, K, C]): SourcingApp[A] = {
    val actionContext = SourcingContext(actionSpec, cSpec)
    val command: Command = input => {
      val commandResponses = CommandConsumer.commandResponseStream(cSpec, input.builder)
      SourcingStream.addSubTopology(actionContext,
                                    input.actionRequests,
                                    input.actionResponses,
                                    commandResponses)
    }
    commands = command :: commands
    topics = topics ++ cSpec.topicNamer.all()
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
