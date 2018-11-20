package client

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.{Properties, UUID}

import command.model.auction.AccountCommand
import command.model.user.UserCommand
import http.{HttpRequest, HttpVerb}
import io.circe.Json
import io.circe.generic.auto._
import io.circe.syntax._
import model.messages.SagaRequest
import model.saga.{ActionCommand, SagaError}
import model.topics
import org.apache.kafka.clients.producer.{KafkaProducer, Producer, ProducerRecord}
import org.slf4j.LoggerFactory
import shared.serdes.JsonSerdes
import shared.utils.{StreamAppConfig, StreamAppUtils}
import saga.dsl._

object App {
  private val logger = LoggerFactory.getLogger(classOf[App])
  def main(args: Array[String]): Unit = {

    val config = StreamAppUtils.getConfig(StreamAppConfig("saga-client-2", "127.0.0.1:9092"))

    for (_ <- 1 to 4) {
      val shouldSucceed = actionSequence("Harry", "Hughley", 1000.0, List(500, 100), 0)
      submitSagaRequest(config, shouldSucceed)

//      println("Press any key to continue...")
//      System.console().reader().read()

      val shouldFailReservation = actionSequence("Peter", "Bogue", 1000.0, List(500, 100, 550), 0)
      submitSagaRequest(config, shouldFailReservation)

//      println("Press any key to continue...")
//      System.console().reader().read()

      val shouldFailConfirmation = actionSequence("Lemuel", "Osorio", 1000.0, List(500, 100, 350), 50)
      submitSagaRequest(config, shouldFailConfirmation)
    }

    //println("Press any key to continue...")
    //System.console().reader().read()
    //()
  }

  private def submitSagaRequest(config: Properties, request: Either[SagaError, SagaRequest[Json]]): Unit =
    request.fold(
      _.messages.toList.foreach(logger.error),
      r => {
        val requestTopic =
          s"${constants.sagaTopicPrefix}${constants.sagaBaseName}-${topics.SagaTopic.request}"

        val sagaSerdes = JsonSerdes.sagaSerdes[Json]
        val producer: Producer[UUID, SagaRequest[Json]] =
          new KafkaProducer[UUID, SagaRequest[Json]](config,
                                                     sagaSerdes.uuid.serializer(),
                                                     sagaSerdes.request.serializer())

        producer.send(new ProducerRecord[UUID, SagaRequest[Json]](requestTopic, r.sagaId, r))
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
