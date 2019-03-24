package io.simplesource.saga.scala.serdes

import java.util.UUID

import io.circe.generic.auto._
import io.simplesource.api.{CommandError, CommandId}
import io.simplesource.api.CommandError.Reason
import io.simplesource.data.{Result, Sequence}
import io.simplesource.kafka.model._
import org.scalatest.{Matchers, WordSpec}

class JsonCommandTests extends WordSpec with Matchers {
  import TestTypes._
  "command serdes" must {
    val serdes =
      JsonSerdes.commandSerdes[UUID, UserCommand]
    val topic = "topic"

    "serialise and deserialise command key UUIDs" in {
      val initial = CommandId.random()
      val ser =
        serdes.commandId().serializer().serialize(topic, initial)
      val de =
        serdes.commandId().deserializer().deserialize(topic, ser)
      de shouldBe initial
    }

    "serialise and deserialise command requests" in {
      val initial =
        new CommandRequest[UUID, UserCommand](
          CommandId.random(),
          UUID.randomUUID(),
                                              Sequence.first(),
          UserCommand.Insert(UUID.randomUUID(), "fn", "ln"))
      val ser = serdes.commandRequest().serializer().serialize(topic, initial)
      val de  = serdes.commandRequest().deserializer().deserialize(topic, ser)
      de shouldBe initial
    }

    "serialise and deserialise command response success" in {
      val initial =
        new CommandResponse(CommandId.random(), UUID.randomUUID(), Sequence.first(), Result.success(Sequence.first().next()))
      val ser = serdes.commandResponse().serializer().serialize(topic, initial)
      val de  = serdes.commandResponse().deserializer().deserialize(topic, ser)
      de shouldBe initial
    }

    "serialise and deserialise command response failure" in {
      val initial =
        new CommandResponse(CommandId.random(),
          UUID.randomUUID(),
          Sequence.first(),
                            Result.failure(CommandError.of(Reason.InvalidCommand, "Invalid command")))
      val ser = serdes.commandResponse().serializer().serialize(topic, initial)
      val de  = serdes.commandResponse().deserializer().deserialize(topic, ser)
      de shouldBe initial
    }
  }
}
