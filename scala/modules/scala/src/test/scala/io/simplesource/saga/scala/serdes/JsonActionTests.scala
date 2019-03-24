package io.simplesource.saga.scala.serdes
import java.util.UUID

import io.circe.Json
import io.circe.generic.auto._
import io.simplesource.api.CommandId
import io.simplesource.data.Result
import io.simplesource.saga.model.action.{ActionCommand, ActionId}
import io.simplesource.saga.model.messages.{ActionRequest, ActionResponse}
import io.simplesource.saga.model.saga.{SagaError, SagaId}
import org.scalatest.{Matchers, WordSpec}

class JsonActionTests extends WordSpec with Matchers {
  import TestTypes._
  import io.circe.syntax._
  "action serdes" must {
    val serdes =
      JsonSerdes.actionSerdes[Json]
    val topic = "topic"

    "serialise and deserialise key UUIDs" in {
      val request =
        ActionRequest
          .builder()
          .sagaId(SagaId.random())
          .actionId(ActionId.random())
          .actionCommand(
            new ActionCommand(CommandId.random(),
                              (UserCommand.Insert(UUID.randomUUID(), "", ""): UserCommand).asJson))
          .actionType("action")
          .build()

      val ser = serdes.request.serializer().serialize(topic, request)
      val de  = serdes.request.deserializer().deserialize(topic, ser)
      de shouldBe request
    }

    "serialise and deserialise sucess responses" in {
      val response =
        new ActionResponse(SagaId.random(), ActionId.random(), CommandId.random(), Result.success(true))
      val ser = serdes.response.serializer().serialize(topic, response)
      val de  = serdes.response.deserializer().deserialize(topic, ser)
      de shouldBe response
    }

    "serialise and deserialise failure responses" in {
      val response = new ActionResponse(SagaId.random(),
                                        ActionId.random(),
                                        CommandId.random(),
                                        Result.failure(SagaError.of(SagaError.Reason.InternalError, "error")))
      val ser = serdes.response.serializer().serialize(topic, response)
      val de  = serdes.response.deserializer().deserialize(topic, ser)
      de shouldBe response
    }
  }
}
