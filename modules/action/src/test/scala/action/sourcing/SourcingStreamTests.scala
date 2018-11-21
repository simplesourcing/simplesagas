package action.sourcing

import java.util.UUID

import action.common.ActionConsumer
import io.circe.Json
import io.circe.generic.auto._
import io.circe.syntax._
import io.simplesource.kafka.util.PrefixResourceNamingStrategy
import model.messages.ActionRequest
import model.saga.ActionCommand
import model.specs.ActionProcessorSpec
import model.topics
import model.topics.{ActionTopic, CommandTopic}
import org.scalatest.{Matchers, WordSpec}
import shared.serdes.JsonSerdes
import shared.serdes.TestTypes.UserCommand
import shared.utils.TopicConfigurer._
import shared.utils.TopicNamer

class SourcingStreamTests extends WordSpec with Matchers {
  import TestUtils._

  val actionSpec = ActionProcessorSpec[Json](
    serdes = JsonSerdes.actionSerdes[Json],
    forStrategy(new PrefixResourceNamingStrategy(""), "action", topics.ActionTopic.all))

  val userSpec = CommandSpec[Json, UserCommand, UUID, UserCommand](
    actionType = "user_action",
    decode = json => json.as[UserCommand],
    serdes = JsonSerdes.commandSerdes[UUID, UserCommand],
    commandMapper = identity,
    keyMapper = _.userId,
    aggregateName = "user",
    timeOutMillis = 30000L
  )

  "action streams" must {
    "turn an action request into a command request" in {
      val commandTopicNamer = TopicNamer.forPrefix("", "user")
      val ctx = SourcingContext(
        actionSpec,
        userSpec,
        TopicNamer.forStrategy(new PrefixResourceNamingStrategy(""), "user", topics.CommandTopic.all))

      val ctxDriver = ContextDriver(
        ctx,
        builder => {
          val actionRequestStream = ActionConsumer.actionRequestStream(actionSpec, builder)
          val commandResponseByAggregate =
            CommandConsumer.commandResponseStream[Json, UserCommand, UUID, UserCommand](userSpec,
                                                                                        commandTopicNamer,
                                                                                        builder)

          val (_ /* error responses */, commandRequests) =
            SourcingStream.handleActionRequest[Json, UserCommand, UUID, UserCommand](
              ctx,
              actionRequestStream,
              commandResponseByAggregate)
          CommandProducer.commandRequest(userSpec, commandTopicNamer, commandRequests)
        }
      )

      val aSerdes = actionSpec.serdes
      val cSerdes = userSpec.serdes

      val sagaId               = UUID.randomUUID()
      val command: UserCommand = UserCommand.Insert(UUID.randomUUID(), "Roscoe", "Marcellus")
      val actionRequest = ActionRequest(sagaId,
                                        UUID.randomUUID(),
                                        ActionCommand(UUID.randomUUID(), command.asJson),
                                        "user_action")

      ctxDriver
        .produce(actionSpec.topicConfig.namer(ActionTopic.request), aSerdes.uuid, aSerdes.request)
        .pipeInput(sagaId, actionRequest)

      val output =
        ctxDriver.readOutput(commandTopicNamer(CommandTopic.request),
                             cSerdes.aggregateKey,
                             cSerdes.commandRequest())
      output.value().command() shouldBe command
    }
  }
}
