package io.simplesource.saga.user.client

import java.time.format.DateTimeFormatter
import java.time.{Duration, LocalDateTime}
import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger

import io.circe.generic.auto._
import io.circe.syntax._
import io.circe.{Encoder, Json}
import io.simplesource.api.CommandId
import io.simplesource.data.Result
import io.simplesource.kafka.dsl.KafkaConfig
import io.simplesource.saga.action.http.HttpRequest
import io.simplesource.saga.action.http.HttpRequest.HttpVerb
import io.simplesource.saga.client.builder.SagaClientBuilder
import io.simplesource.saga.client.dsl.SagaDsl._
import io.simplesource.saga.model.action.{ActionCommand, ActionId}
import io.simplesource.saga.model.api.SagaAPI
import io.simplesource.saga.model.messages.SagaRequest
import io.simplesource.saga.model.saga.{SagaError, SagaId}
import io.simplesource.saga.scala.serdes.JsonSerdes
import io.simplesource.saga.user.action.App.Key
import io.simplesource.saga.user.action.HttpClient
import io.simplesource.saga.user.command.model.auction.AccountCommand
import io.simplesource.saga.user.command.model.user.UserCommand
import io.simplesource.saga.user.shared.TopicUtils
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._

object App {
  private val logger                       = LoggerFactory.getLogger(classOf[App])
  private val responseCount: AtomicInteger = new AtomicInteger(0)

  def main(args: Array[String]): Unit = {

    val sagaClientBuilder: SagaClientBuilder[Json] =
      new SagaClientBuilder[Json](
        (kafkaConfigBuilder: KafkaConfig.Builder) =>
          kafkaConfigBuilder
            .withKafkaApplicationId("saga-app-1")
            .withKafkaBootstrap("127.0.0.1:9092"))
    val api: SagaAPI[Json] = sagaClientBuilder
      .withSerdes(JsonSerdes.sagaSerdes[Json])
      .withTopicConfig(TopicUtils.buildSteps(constants.sagaTopicPrefix, constants.sagaBaseName))
      .withClientId("saga-client-1")
      .build()

    for (_ <- 1 to 3) {
      val shouldSucceed =
        actionSequence("Harry", "Hughley", 1000.0, List(500, 100), 0)
      submitSagaRequest(api, shouldSucceed)

      val shouldFailReservation =
        actionSequence("Peter", "Bogue", 1000.0, List(500, 100, 550), 0)
      submitSagaRequest(api, shouldFailReservation)

      val shouldFailConfirmation =
        actionSequence("Lemuel", "Osorio", 1000.0, List(500, 100, 350), 50)
      submitSagaRequest(api, shouldFailConfirmation)
    }
  }

  private def submitSagaRequest(sagaApi: SagaAPI[Json], request: Result[SagaError, SagaRequest[Json]]): Unit =
    request.fold[Unit](
      es => es.map(e => logger.error(e.getMessage)),
      r => {
        for {
          _        <- sagaApi.submitSaga(r)
          response <- sagaApi.getSagaResponse(r.sagaId, Duration.ofSeconds(60L))
          _ = {
            val count = responseCount.incrementAndGet()
            logger.info(s"Saga response $count received:\n$response")
          }
        } yield ()
        ()
      }
    )

  def actionSequence(firstName: String,
                     lastName: String,
                     funds: BigDecimal,
                     amounts: List[BigDecimal],
                     adjustment: BigDecimal = 0): Result[SagaError, SagaRequest[Json]] = {
    val accountId = UUID.randomUUID()

    val builder = SagaBuilder.create[Json]

    val addUser = builder.addAction(
      ActionId.random(),
      constants.userActionType,
      (UserCommand.Insert(userId = UUID.randomUUID(), firstName, lastName): UserCommand).asJson)

    val createAccount = builder.addAction(
      ActionId.random(),
      constants.accountActionType,
      (AccountCommand
        .CreateAccount(accountId = accountId, userName = s"$firstName $lastName", funds = 1000): AccountCommand).asJson
    )

    val amountsWithIds = amounts.map((_, ActionId.random(), UUID.randomUUID()))

    val reservations = amountsWithIds.map {
      case (amount, actionId, resId) =>
        builder.addAction(
          actionId,
          constants.accountActionType,
          (AccountCommand.ReserveFunds(accountId = accountId,
                                       reservationId = resId,
                                       description = s"res-${resId.toString.take(4)}",
                                       amount = amount): AccountCommand).asJson,
          (AccountCommand
            .CancelReservation(accountId = accountId, reservationId = resId): AccountCommand).asJson
        )
    }

    val confirmations = amountsWithIds.map {
      case (amount, _, resId) =>
        builder.addAction(
          ActionId.random(),
          constants.accountActionType,
          (AccountCommand.ConfirmReservation(accountId = accountId,
                                             reservationId = resId,
                                             finalAmount = amount + adjustment): AccountCommand).asJson
        )
    }

    val testAsyncInvoke: SubSaga[Json] = builder.addAction(
      ActionId.random(),
      "async_test_action_type",
      s"Hello World, time is: ${LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME)}".asJson)

    val v: HttpRequest[Key, String] = HttpRequest.ofWithBody[Key, String](
      Key("fx"),
      HttpVerb.Get,
      "https://api.exchangeratesapi.io/latest",
      "fx_rates",
      null)

    import io.simplesource.saga.user.action.App.Key
    implicit val encoder: Encoder[HttpRequest[Key, String]] =
      HttpClient.httpRequest[Key, String]._1

    val testHttpInvoke: SubSaga[Json] = builder.addAction(ActionId.random(), "http_action_type", v.asJson)

    testAsyncInvoke
      .andThen(testHttpInvoke)
      .andThen(addUser)
      .andThen(createAccount)
      .andThen(inSeries(reservations.asJava))
      .andThen(inSeries(confirmations.asJava))

    builder.build().map(s => new SagaRequest(SagaId.random(), s))
  }

}
