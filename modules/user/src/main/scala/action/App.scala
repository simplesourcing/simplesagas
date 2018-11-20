package action
import java.util.UUID

import action.async.{AsyncApp, AsyncOutput, AsyncSerdes, AsyncSpec}
import action.sourcing._
import command.model.auction.AccountCommand
import command.model.user.UserCommand
import io.circe.Json
import io.circe.generic.auto._
import io.simplesource.kafka.util.PrefixResourceNamingStrategy
import model.specs.ActionProcessorSpec
import model.topics
import org.apache.kafka.common.serialization.Serdes
import shared.utils.{StreamAppConfig, TopicNamer}
import shared.serdes.JsonSerdes
import http._
import http.implicits._
import scala.concurrent.ExecutionContext.Implicits.global

import scala.concurrent.Future

object App {
  val sourcingConfig =
    StreamAppConfig(appId = "sourcing-action-processor-1", bootstrapServers = "127.0.0.1:9092")
  val asyncConfig = StreamAppConfig(appId = "async-action-processor-1", bootstrapServers = "127.0.0.1:9092")

  def main(args: Array[String]): Unit = {
    startSourcingActionProcessor()
    startAsyncActionProcessor()
  }

  def startSourcingActionProcessor(): Unit = {
    SourcingApp[Json](actionProcessorSpec)
      .addCommand(accountSpec)
      .addCommand(userSpec)
      .run(sourcingConfig)
  }

  def startAsyncActionProcessor(): Unit = {
    AsyncApp[Json](actionProcessorSpec)
      .addAsync(asyncSpec)
      .addHttpProcessor(httpSpec)
      .run(asyncConfig)
  }

  lazy val actionProcessorSpec = ActionProcessorSpec[Json](
    serdes = JsonSerdes.actionSerdes[Json],
    TopicNamer.forStrategy(new PrefixResourceNamingStrategy(constants.actionTopicPrefix),
                           constants.sagaActionBaseName,
                           topics.ActionTopic.all)
  )

  lazy val userSpec = CommandSpec[Json, UserCommand, UUID, UserCommand](
    actionType = constants.userActionType,
    decode = json => json.as[UserCommand],
    commandMapper = identity,
    keyMapper = _.userId,
    topicNamer = TopicNamer.forStrategy(new PrefixResourceNamingStrategy(constants.commandTopicPrefix),
                                        topicBaseName = constants.userAggregateName,
                                        allTopics = topics.CommandTopic.all),
    serdes = JsonSerdes.commandSerdes[UUID, UserCommand],
    aggregateName = constants.userAggregateName,
    timeOutMillis = 30000L
  )

  lazy val accountSpec = CommandSpec[Json, AccountCommand, UUID, AccountCommand](
    actionType = constants.accountActionType,
    decode = json => json.as[AccountCommand],
    commandMapper = identity,
    keyMapper = _.accountId,
    topicNamer = TopicNamer.forStrategy(new PrefixResourceNamingStrategy(constants.commandTopicPrefix),
                                        topicBaseName = constants.accountAggregateName,
                                        allTopics = topics.CommandTopic.all),
    serdes = JsonSerdes.commandSerdes[UUID, AccountCommand],
    aggregateName = constants.accountAggregateName,
    timeOutMillis = 30000L
  )

  lazy val asyncSpec = AsyncSpec[Json, String, String, String, String](
    inputDecoder = a => {
      val decoded = a.as[String]
      decoded
    },
    keyMapper = i => i.toLowerCase.take(3),
    asyncFunction = i => Future.successful(s"${i.length.toString}: $i"),
    actionType = "async_test_action_type",
    groupId = asyncConfig.appId,
    outputSpec = Some(
      AsyncOutput(o => Some(Right(o)),
                  AsyncSerdes(Serdes.String(), Serdes.String()),
                  _ => Some("async_test_topic"),
                  topicNames = List("async_test_topic"))),
  )

  // Http currency fetch example
  final case class Key(id: String)
  type Body   = Option[String]
  type Input  = Json
  type Output = Json
  final case class FXRates(date: String, base: String, rates: Map[String, BigDecimal])

  lazy val httpSpec = HttpSpec[Input, Key, Body, Output, FXRates](
    "http_action_type",
    _.as[HttpRequest[Key, Body]],
    HttpClient.requester[Key, Body, Output],
    asyncConfig.appId,
    Some(
      HttpOutput(o => Some(o.as[FXRates]),
                 AsyncSerdes(JsonSerdes.serdeFromCodecs[Key], JsonSerdes.serdeFromCodecs[FXRates]),
                 topicNames = List("fx_rates")))
  )
}
