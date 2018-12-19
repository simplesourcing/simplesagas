//package action.sourcing
//
//import java.util.UUID
//
//import action.common.ActionConsumer
//import io.circe.Json
//import io.circe.generic.auto._
//import io.circe.syntax._
//import model.messages.ActionRequest
//import model.saga.ActionCommand
//import model.specs.ActionProcessorSpec
//import shared.topics.TopicTypes.{ActionTopic, CommandTopic}
//import org.scalatest.{Matchers, WordSpec}
//import io.simplesource.saga.scala.serdes.JsonSerdes
//import io.simplesource.saga.scala.serdes.TestTypes.UserCommand
//import shared.topics.TopicNamer
//class SourcingStreamTests extends WordSpec with Matchers {
//  import TestUtils._
//
//  private val actionSpec        = ActionProcessorSpec[Json](serdes = JsonSerdes.actionSerdesScala[Json])
//  private val actionTopicNamer  = TopicNamer.forPrefix("", "action")
//  private val commandTopicNamer = TopicNamer.forPrefix("", "user")
//
//  private val userSpec = CommandSpec[Json, UserCommand, UUID, UserCommand](
//    actionType = "user_action",
//    decode = json => json.as[UserCommand],
//    serdes = JsonSerdes.commandSerdes[UUID, UserCommand],
//    commandMapper = identity,
//    keyMapper = _.userId,
//    aggregateName = "user",
//    timeOutMillis = 30000L
//  )
//
//  "action streams" must {
//    "turn an action request into a command request" in {
//      val ctx = SourcingContext(actionSpec, userSpec, actionTopicNamer, commandTopicNamer)
//
//      val ctxDriver = ContextDriver(
//        ctx,
//        builder => {
//          val actionRequestStream = ActionConsumer.actionRequestStream(actionSpec, actionTopicNamer, builder)
//          val commandResponseByAggregate =
//            CommandConsumer.commandResponseStream[Json, UserCommand, UUID, UserCommand](userSpec,
//                                                                                        commandTopicNamer,
//                                                                                        builder)
//
//          val (_ /* error responses */, commandRequests) =
//            SourcingStream.handleActionRequest[Json, UserCommand, UUID, UserCommand](
//              ctx,
//              actionRequestStream,
//              commandResponseByAggregate)
//          CommandProducer.commandRequest(userSpec, commandTopicNamer, commandRequests)
//        }
//      )
//
//      val aSerdes = actionSpec.serdes
//      val cSerdes = userSpec.serdes
//
//      val sagaId               = UUID.randomUUID()
//      val command: UserCommand = UserCommand.Insert(UUID.randomUUID(), "Roscoe", "Marcellus")
//      val actionRequest = ActionRequest(sagaId,
//                                        UUID.randomUUID(),
//                                        ActionCommand(UUID.randomUUID(), command.asJson),
//                                        "user_action")
//
//      ctxDriver
//        .produce(actionTopicNamer(ActionTopic.request), aSerdes.uuid, aSerdes.request)
//        .pipeInput(sagaId, actionRequest)
//
//      val output =
//        ctxDriver.readOutput(commandTopicNamer(CommandTopic.request),
//                             cSerdes.aggregateKey,
//                             cSerdes.commandRequest())
//      output.value().command() shouldBe command
//    }
//  }
//}
