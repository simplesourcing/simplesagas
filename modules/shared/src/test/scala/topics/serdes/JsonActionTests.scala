package topics.serdes
import java.util.UUID

import cats.data.NonEmptyList
import io.circe.Json
import io.circe.generic.auto._
import model.messages.{ActionRequest, ActionResponse}
import model.saga.ActionCommand
import org.scalatest.{Matchers, WordSpec}

class JsonActionTests extends WordSpec with Matchers {
  import TestTypes._
  import io.circe.syntax._
  "action shared.serdes" must {
    val serdes =
      JsonSerdes.actionSerdes[Json]
    val topic = "topic"

    "serialise and deserialise key UUIDs" in {
      val request = ActionRequest(
        UUID.randomUUID(),
        UUID.randomUUID(),
        ActionCommand(UUID.randomUUID(), (UserCommand.Insert(UUID.randomUUID(), "", ""): UserCommand).asJson),
        "action")
      val ser = serdes.request.serializer().serialize(topic, request)
      val de  = serdes.request.deserializer().deserialize(topic, ser)
      de shouldBe request
    }

    "serialise and deserialise sucess responses" in {
      val response = ActionResponse(UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID(), Right(()))
      val ser      = serdes.response.serializer().serialize(topic, response)
      val de       = serdes.response.deserializer().deserialize(topic, ser)
      de shouldBe response
    }

    "serialise and deserialise failure responses" in {
      val response = ActionResponse(UUID.randomUUID(),
                                    UUID.randomUUID(),
                                    UUID.randomUUID(),
                                    Left(model.saga.SagaError(NonEmptyList.of("First", "Second", "Third"))))
      val ser = serdes.response.serializer().serialize(topic, response)
      val de  = serdes.response.deserializer().deserialize(topic, ser)
      de shouldBe response
    }
  }
}
