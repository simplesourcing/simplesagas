package io.simplesource.saga.user.client

import java.time.{Duration, LocalDateTime}
import java.time.format.DateTimeFormatter
import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger

import io.simplesource.saga.user.action.App.Key
import io.simplesource.saga.user.command.model.auction.AccountCommand
import io.simplesource.saga.user.command.model.user.UserCommand
import io.circe.{Encoder, Json}
import io.circe.generic.auto._
import io.circe.syntax._
import io.simplesource.data.Result
import io.simplesource.kafka.dsl.KafkaConfig
import io.simplesource.saga.action.http.HttpRequest
import io.simplesource.saga.action.http.HttpRequest.HttpVerb
import io.simplesource.saga.model.api.SagaAPI
import io.simplesource.saga.model.messages.SagaRequest
import io.simplesource.saga.model.saga.{ActionCommand, SagaError}
import io.simplesource.saga.saga.builder.SagaClientBuilder
import org.slf4j.LoggerFactory
import io.simplesource.saga.user.shared.TopicUtils
import io.simplesource.saga.scala.serdes.JsonSerdes

import scala.collection.JavaConverters._
import io.simplesource.saga.saga.dsl.SagaDsl._
import io.simplesource.saga.user.action.HttpClient

object App {
  private val logger                       = LoggerFactory.getLogger(classOf[App])
  private val responseCount: AtomicInteger = new AtomicInteger(0)

  def main(args: Array[String]): Unit = {

    val sagaClientBuilder: SagaClientBuilder[Json] = new SagaClientBuilder[Json](
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
      val shouldSucceed = actionSequence("Harry", "Hughley", 1000.0, List(500, 100), 0)
      submitSagaRequest(api, shouldSucceed)

      val shouldFailReservation = actionSequence("Peter", "Bogue", 1000.0, List(500, 100, 550), 0)
      submitSagaRequest(api, shouldFailReservation)

      val shouldFailConfirmation = actionSequence("Lemuel", "Osorio", 1000.0, List(500, 100, 350), 50)
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
      UUID.randomUUID(),
      constants.userActionType,
      new ActionCommand(
        UUID.randomUUID(),
        (UserCommand.Insert(userId = UUID.randomUUID(), firstName, lastName): UserCommand).asJson)
    )

    val createAccount = builder.addAction(
      UUID.randomUUID(),
      constants.accountActionType,
      new ActionCommand(UUID.randomUUID(),
                        (AccountCommand
                          .CreateAccount(accountId = accountId,
                                         userName = s"$firstName $lastName",
                                         funds = 1000): AccountCommand).asJson)
    )

    val amountsWithIds = amounts.map((_, UUID.randomUUID(), UUID.randomUUID()))

    val reservations = amountsWithIds.map {
      case (amount, actionId, resId) =>
        builder.addAction(
          actionId,
          constants.accountActionType,
          new ActionCommand(
            UUID.randomUUID(),
            (AccountCommand.ReserveFunds(accountId = accountId,
                                         reservationId = resId,
                                         description = s"res-${resId.toString.take(4)}",
                                         amount = amount): AccountCommand).asJson
          ),
          new ActionCommand(
            UUID.randomUUID(),
            (AccountCommand
              .CancelReservation(accountId = accountId, reservationId = resId): AccountCommand).asJson)
        )
    }

    val confirmations = amountsWithIds.map {
      case (amount, _, resId) =>
        builder.addAction(
          UUID.randomUUID(),
          constants.accountActionType,
          new ActionCommand(
            UUID.randomUUID(),
            (AccountCommand.ConfirmReservation(accountId = accountId,
                                               reservationId = resId,
                                               finalAmount = amount + adjustment): AccountCommand).asJson)
        )
    }

    val testAsyncInvoke: SubSaga[Json] = builder.addAction(
      UUID.randomUUID(),
      "async_test_action_type",
      new ActionCommand(
        UUID.randomUUID(),
        s"Hello World, time is: ${LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME)}".asJson)
    )

    val v: HttpRequest[Key, String] = HttpRequest.ofWithBody[Key, String](
      Key("fx"),
      HttpVerb.Get,
      "https://api.exchangeratesapi.io/latest",
      "fx_rates",
      null)

    import io.simplesource.saga.user.action.App.Key
    implicit val encoder: Encoder[HttpRequest[Key, String]] = HttpClient.httpRequest[Key, String]._1

    val testHttpInvoke: SubSaga[Json] = builder.addAction(
      UUID.randomUUID(),
      "http_action_type",
      new ActionCommand(
        UUID.randomUUID(),
        v.asJson
      )
    )

    testAsyncInvoke
      .andThen(testHttpInvoke)
      .andThen(addUser)
      .andThen(createAccount)
      .andThen(inSeries(reservations.asJava))
      .andThen(inSeries(confirmations.asJava))

    builder.build().map(s => new SagaRequest(UUID.randomUUID(), s))
  }

}
