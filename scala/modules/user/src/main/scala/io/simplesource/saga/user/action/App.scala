package io.simplesource.saga.user.action

import java.time.Duration
import java.time.temporal.ChronoUnit
import java.util.{Optional, UUID}

import io.circe.Json
import io.circe.generic.auto._
import io.simplesource.data.Result
import io.simplesource.kafka.spec.TopicSpec
import io.simplesource.saga.action.async.{AsyncOutput, AsyncSerdes, AsyncSpec, Callback}
import io.simplesource.saga.action.http.{HttpApp, HttpOutput, HttpRequest, HttpSpec}
import io.simplesource.saga.action.sourcing.{ActionCommandMapping, CommandSpec, SourcingApp}
import io.simplesource.saga.scala.serdes.{JsonSerdes, ProductCodecs}
import io.simplesource.saga.shared.topics.{TopicCreation, TopicNamer}
import io.simplesource.saga.shared.utils.StreamAppConfig
import io.simplesource.saga.user.command.model.auction.AccountCommand
import io.simplesource.saga.user.command.model.user.UserCommand
import io.simplesource.saga.user.shared.TopicUtils
import org.apache.kafka.common.serialization.Serdes

import scala.collection.JavaConverters._

object App {
  val sourcingConfig =
    new StreamAppConfig("sourcing-action-processor-1", "127.0.0.1:9092")
  val asyncConfig =
    new StreamAppConfig("async-action-processor-1", "127.0.0.1:9092")

  def main(args: Array[String]): Unit = {
    startSourcingActionProcessor()
    startAsyncActionProcessor()
  }

  def startSourcingActionProcessor(): Unit = {
    new SourcingApp[Json](
      JsonSerdes.actionSerdes[Json],
      TopicUtils.buildSteps(constants.actionTopicPrefix, constants.sagaActionBaseName)
    ).addCommand(accountSpec,
                  TopicUtils.buildSteps(constants.commandTopicPrefix, constants.accountAggregateName))
      .addCommand(userSpec, TopicUtils.buildSteps(constants.commandTopicPrefix, constants.userAggregateName))
      .run(sourcingConfig)
  }

  def startAsyncActionProcessor(): Unit = {
    new HttpApp[Json](JsonSerdes.actionSerdes[Json],
                      TopicUtils.buildSteps(constants.actionTopicPrefix, constants.sagaActionBaseName))
      .addAsync(asyncSpec)
      .addHttpProcessor(httpSpec)
      .run(asyncConfig)
  }

  implicit class EOps[E, A](eea: Either[E, A]) {
    def toResult: Result[E, A] =
      eea.fold(e => Result.failure(e), a => Result.success(a))
  }

  lazy val userSpec: CommandSpec[Json, UUID, UserCommand] = CommandSpec
    .builder[Json, UUID, UserCommand]()
    .aggregateName(constants.userAggregateName)
    .commandSerdes(JsonSerdes.commandSerdes[UUID, UserCommand])
    .commandTopicNamer(TopicNamer.forPrefix(constants.commandTopicPrefix, constants.userAggregateName))
    .timeOutMillis(30000L)
    .build()
    .handleAction(
      ActionCommandMapping.builder[Json, UserCommand, UUID, UserCommand]()
          .actionType(constants.userActionType)
          .decode(json => json.as[UserCommand].toResult.errorMap(e => e))
          .commandMapper(a => a)
          .keyMapper(a => a.userId)
          .build()
    )

  lazy val accountSpec = CommandSpec
    .builder[Json, UUID, AccountCommand]()
    .aggregateName(constants.accountAggregateName)
    .commandSerdes(JsonSerdes.commandSerdes[UUID, AccountCommand])
    .commandTopicNamer(TopicNamer.forPrefix(constants.commandTopicPrefix, constants.accountAggregateName))
    .timeOutMillis(30000L)
    .build()
    .handleAction(
      ActionCommandMapping.builder[Json, AccountCommand, UUID, AccountCommand]()
        .actionType(constants.accountActionType)
        .decode(json => json.as[AccountCommand].toResult.errorMap(e => e))
        .commandMapper(a => a)
        .keyMapper(a => a.accountId)
        .build()
    )

  lazy val asyncSpec = new AsyncSpec[Json, String, String, String, String](
    "async_test_action_type",
    (a: Json) => {
      val decoded = a.as[String]
      decoded.toResult.errorMap(e => e)
    },
    i => i.toLowerCase.take(3),
    (i: String, callBack: Callback[String]) => {
      callBack.complete(Result.success(s"${i.length.toString}: $i"))
    }, //i => Future.successful(s"${i.length.toString}: $i"),
    asyncConfig.appId,
    Optional.of(
      new AsyncOutput(
        o => Optional.of(Result.success(o)),
        new AsyncSerdes(Serdes.String(), Serdes.String()),
        _ => Optional.of("async_test_topic"),
        List(new TopicCreation("async_test_topic", new TopicSpec(6, 1, Map.empty[String, String].asJava))).asJava
      )),
    Optional.of(Duration.of(60, ChronoUnit.SECONDS))
  )

  // Http currency fetch example
  final case class Key(id: String)
  type Body   = Option[String]
  type Input  = Json
  type Output = Json
  final case class FXRates(date: String, base: String, rates: Map[String, BigDecimal])

  import io.circe.generic.auto._
  import scala.concurrent.ExecutionContext.Implicits.global
  implicit val decoder = HttpClient.httpRequest[Key, Body]._2

  lazy val httpSpec = new HttpSpec[Input, Key, Body, Output, FXRates](
    "http_action_type",
    _.as[HttpRequest[Key, Body]].toResult.map(x => x).errorMap(e => e),
    HttpClient.requester[Key, Body, Output],
    asyncConfig.appId,
    Optional.of(
      new HttpOutput(
        (o: Input) => Optional.of(o.as[FXRates].toResult.errorMap(e => e)),
        new AsyncSerdes(ProductCodecs.serdeFromCodecs[Key], ProductCodecs.serdeFromCodecs[FXRates]),
        List(new TopicCreation("fx_rates", new TopicSpec(6, 1, Map.empty[String, String].asJava))).asJava
      )),
    Optional.of(Duration.of(60, ChronoUnit.SECONDS))
  )
}
