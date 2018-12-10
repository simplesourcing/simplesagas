package client

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger

import command.model.auction.AccountCommand
import command.model.user.UserCommand
import http.{HttpRequest, HttpVerb}
import io.circe.Json
import io.circe.generic.auto._
import io.circe.syntax._
import model.api.SagaAPI
import model.messages.SagaRequest
import model.saga.{ActionCommand, SagaError}
import org.slf4j.LoggerFactory
import saga.SagaClient
import saga.dsl._
import shared.TopicUtils
import shared.serdes.JsonSerdes

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

object App {
  private val logger                       = LoggerFactory.getLogger(classOf[App])
  private val responseCount: AtomicInteger = new AtomicInteger(0)

  def main(args: Array[String]): Unit = {

    val sagaClient = SagaClient(kafkaConfigBuilder => kafkaConfigBuilder.withKafkaBootstrap("127.0.0.1:9092"))

    val api: SagaAPI[Json] = sagaClient.createSagaApi[Json] { builder =>
      builder
        .withSerdes(JsonSerdes.sagaSerdes[Json])
        .withTopicConfig(TopicUtils.buildSteps(constants.sagaTopicPrefix, constants.sagaBaseName))
        .withClientId("saga-client-1")
    }

    for (_ <- 1 to 5) {
      val shouldSucceed = actionSequence("Harry", "Hughley", 1000.0, List(500, 100), 0)
      submitSagaRequest(api, shouldSucceed)

      val shouldFailReservation = actionSequence("Peter", "Bogue", 1000.0, List(500, 100, 550), 0)
      submitSagaRequest(api, shouldFailReservation)

      val shouldFailConfirmation = actionSequence("Lemuel", "Osorio", 1000.0, List(500, 100, 350), 50)
      submitSagaRequest(api, shouldFailConfirmation)
    }
  }

  private def submitSagaRequest(sagaApi: SagaAPI[Json], request: Either[SagaError, SagaRequest[Json]]): Unit =
    request.fold(
      _.messages.toList.foreach(logger.error),
      r => {
        for {
          _        <- sagaApi.submitSaga(r)
          response <- sagaApi.getSagaResponse(r.sagaId, 60.seconds)
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
                     adjustment: BigDecimal = 0): Either[SagaError, SagaRequest[Json]] = {
    val accountId = UUID.randomUUID()

    val builder = SagaBuilder[Json]()

    val addUser = builder.addAction(
      actionType = constants.userActionType,
      actionId = UUID.randomUUID(),
      command = ActionCommand(
        UUID.randomUUID(),
        (UserCommand.Insert(userId = UUID.randomUUID(), firstName, lastName): UserCommand).asJson)
    )

    val createAccount = builder.addAction(
      actionType = constants.accountActionType,
      actionId = UUID.randomUUID(),
      command = ActionCommand(UUID.randomUUID(),
                              (AccountCommand
                                .CreateAccount(accountId = accountId,
                                               userName = s"$firstName $lastName",
                                               funds = 1000): AccountCommand).asJson)
    )

    val amountsWithIds = amounts.map((_, UUID.randomUUID(), UUID.randomUUID()))

    val reservations = amountsWithIds.map {
      case (amount, actionId, resId) =>
        builder.addAction(
          actionType = constants.accountActionType,
          actionId = actionId,
          command = ActionCommand(
            UUID.randomUUID(),
            (AccountCommand.ReserveFunds(accountId = accountId,
                                         reservationId = resId,
                                         description = s"res-${resId.toString.take(4)}",
                                         amount = amount): AccountCommand).asJson
          ),
          undoAction = Some(
            ActionCommand(
              UUID.randomUUID(),
              (AccountCommand
                .CancelReservation(accountId = accountId, reservationId = resId): AccountCommand).asJson))
        )
    }

    val confirmations = amountsWithIds.map {
      case (amount, _, resId) =>
        builder.addAction(
          actionType = constants.accountActionType,
          actionId = UUID.randomUUID(),
          command = ActionCommand(
            UUID.randomUUID(),
            (AccountCommand.ConfirmReservation(accountId = accountId,
                                               reservationId = resId,
                                               finalAmount = amount + adjustment): AccountCommand).asJson)
        )
    }

    val testAsyncInvoke = builder.addAction(
      actionType = "async_test_action_type",
      actionId = UUID.randomUUID(),
      command = ActionCommand(
        UUID.randomUUID(),
        s"Hello World, time is: ${LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME)}".asJson)
    )

    import action.App.Key
    val testHttpInvoke = builder.addAction(
      actionType = "http_action_type",
      actionId = UUID.randomUUID(),
      command = ActionCommand(
        UUID.randomUUID(),
        HttpRequest[Key, Option[String]](Key("fx"),
                                         HttpVerb.Get,
                                         "https://api.exchangeratesapi.io/latest",
                                         Map.empty,
                                         None,
                                         Some("fx_rates")).asJson
      )
    )

    testAsyncInvoke ~> testHttpInvoke ~> addUser ~> createAccount ~> inSeries(reservations) ~> inSeries(
      confirmations)

    builder.build().map(s => SagaRequest(UUID.randomUUID(), s))
  }

}
