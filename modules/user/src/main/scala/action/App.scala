package action
import java.util.{Optional, UUID}

import io.simplesource.saga.action.async._
import io.simplesource.saga.action.sourcing._
import command.model.auction.AccountCommand
import command.model.user.UserCommand
import io.simplesource.saga.action.http.HttpSpec
import io.circe.Json
import io.circe.generic.auto._
import io.simplesource.data.Result
import org.apache.kafka.common.serialization.Serdes
import io.simplesource.saga.shared.utils.StreamAppConfig
import shared.serdes.{JsonSerdeUtils, JsonSerdes}
import io.simplesource.kafka.spec.TopicSpec
import io.simplesource.saga.action.http.{HttpApp, HttpOutput, HttpRequest}
import io.simplesource.saga.shared.topics.TopicCreation
import shared.TopicUtils

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.collection.JavaConverters._

object App {
  val sourcingConfig =
    new StreamAppConfig("sourcing-action-processor-1", "127.0.0.1:9092")
  val asyncConfig = new StreamAppConfig("async-action-processor-1",  "127.0.0.1:9092")

  def main(args: Array[String]): Unit = {
    startSourcingActionProcessor()
    startAsyncActionProcessor()
  }

  def startSourcingActionProcessor(): Unit = {
    new SourcingApp[Json](
      JsonSerdes.actionSerdes[Json],
      TopicUtils.buildStepsJ(constants.actionTopicPrefix, constants.sagaActionBaseName)
    ).addCommand(accountSpec,
                  TopicUtils.buildStepsJ(constants.commandTopicPrefix, constants.accountAggregateName))
      .addCommand(userSpec, TopicUtils.buildStepsJ(constants.commandTopicPrefix, constants.userAggregateName))
      .run(sourcingConfig)
  }

  def startAsyncActionProcessor(): Unit = {
    new HttpApp[Json](JsonSerdes.actionSerdes[Json],
                   TopicUtils.buildStepsJ(constants.actionTopicPrefix, constants.sagaActionBaseName))
      .addAsync(asyncSpec)
      .addHttpProcessor(httpSpec)
      .run(asyncConfig)
  }

  implicit class EOps[E, A](eea: Either[E, A]) {
    def toResult(): Result[E, A] = eea.fold(e => Result.failure(e), a => Result.success(a))
  }

  lazy val userSpec = new CommandSpec[Json, UserCommand, UUID, UserCommand](
    constants.userActionType,
    json => json.as[UserCommand].toResult().errorMap(e => e),
    (a: UserCommand) => a,
    _.userId,
    JsonSerdes.commandSerdes[UUID, UserCommand],
    constants.userAggregateName,
    30000L
  )

  lazy val accountSpec = new CommandSpec[Json, AccountCommand, UUID, AccountCommand](
    constants.accountActionType,
    json => json.as[AccountCommand].toResult().errorMap(e => e),
    (a: AccountCommand) => a,
    _.accountId,
    JsonSerdes.commandSerdes[UUID, AccountCommand],
    constants.accountAggregateName,
    30000L
  )

  lazy val asyncSpec = new AsyncSpec[Json, String, String, String, String](
    "async_test_action_type",
    (a: Json) => {
      val decoded = a.as[String]
      decoded.toResult().errorMap(e => e)
    },
    i => i.toLowerCase.take(3),
    (i: String, callBack: Callback[String]) => { callBack.complete(Result.success(s"${i.length.toString}: $i"))},   //i => Future.successful(s"${i.length.toString}: $i"),
    asyncConfig.appId,
    Optional.of(
      new AsyncOutput(
        o => Optional.of(Result.success(o)),
        new AsyncSerdes(Serdes.String(), Serdes.String()),
        _ => Optional.of("async_test_topic"),
          List(new TopicCreation("async_test_topic",
            new TopicSpec(6, 1, Map.empty[String, String].asJava))).asJava
      )),
  )

  // Http currency fetch example
  final case class Key(id: String)
  type Body   = Option[String]
  type Input  = Json
  type Output = Json
  final case class FXRates(date: String, base: String, rates: Map[String, BigDecimal])

  lazy val httpSpec = new HttpSpec[Input, Key, Body, Output, FXRates](
    "http_action_type",
    _.as[HttpRequest[Key, Body]],
    HttpClient.requester[Key, Body, Output],
    asyncConfig.appId,
    Optional.of(
      new HttpOutput(
        (o: Input) => Optional.of(o.as[FXRates].toResult().errorMap(e => e)),
        new AsyncSerdes(JsonSerdeUtils.serdeFromCodecs[Key], JsonSerdeUtils.serdeFromCodecs[FXRates]),
        List(new TopicCreation("fx_rates",
          new TopicSpec(6, 1, Map.empty[String, String].asJava))).asJava
      ))
  )
}
