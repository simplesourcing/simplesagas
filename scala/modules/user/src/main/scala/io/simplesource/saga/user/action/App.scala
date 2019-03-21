package io.simplesource.saga.user.action

import java.time.Duration
import java.time.temporal.ChronoUnit
import java.util.{Optional, UUID}

import io.circe.Json
import io.circe.generic.auto._
import io.simplesource.data.{Result, Sequence}
import io.simplesource.kafka.spec.TopicSpec
import io.simplesource.saga.action.async.{AsyncOutput, AsyncSerdes, AsyncSpec, Callback}
import io.simplesource.saga.action.http.{HttpApp, HttpOutput, HttpRequest, HttpSpec}
import io.simplesource.saga.action.sourcing.{CommandSpec, SourcingApp}
import io.simplesource.saga.scala.serdes.{JsonSerdes, ProductCodecs}
import io.simplesource.saga.shared.topics.TopicCreation
import io.simplesource.saga.shared.utils.StreamAppConfig
import io.simplesource.saga.user.command.model.auction.{AccountCommand, AccountCommandInfo}
import io.simplesource.saga.user.command.model.user.{UserCommand, UserCommandInfo}
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
    new SourcingApp[Json](JsonSerdes.actionSerdes[Json],
                          TopicUtils.buildSteps(constants.actionTopicPrefix, constants.sagaActionBaseName))
      .addCommand(accountSpec,
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

  lazy val userSpec = new CommandSpec[Json, UserCommandInfo, UUID, UserCommand](
    constants.userActionType,
    json => json.as[UserCommandInfo].toResult.errorMap(e => e),
    _.command,
    _.userId,
    i => Sequence.position(i.sequence),
    JsonSerdes.commandSerdes[UUID, UserCommand],
    30000L
  )

  lazy val accountSpec =
    new CommandSpec[Json, AccountCommandInfo, UUID, AccountCommand](
      constants.accountActionType,
      json => json.as[AccountCommandInfo].toResult.errorMap(e => e),
      _.command,
      _.accountId,
      i => Sequence.position(i.sequence),
      JsonSerdes.commandSerdes[UUID, AccountCommand],
      30000L
    )

  lazy val asyncSpec = new AsyncSpec[Json, String, String, String, String](
    "async_test_action_type",
    (a: Json) => {
      val decoded = a.as[String]
      decoded.toResult.errorMap(e => e)
    },
    (i: String, callBack: Callback[String]) => {
      callBack.complete(Result.success(s"${i.length.toString}: $i"))
    }, //i => Future.successful(s"${i.length.toString}: $i"),
    asyncConfig.appId,
    Optional.of(
      new AsyncOutput(
        o => Optional.of(Result.success(o)),
        new AsyncSerdes(Serdes.String(), Serdes.String()),
        i => i.toLowerCase.take(3),
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
