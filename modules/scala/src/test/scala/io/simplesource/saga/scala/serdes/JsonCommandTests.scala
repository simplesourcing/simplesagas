package io.simplesource.saga.scala.serdes

import java.util.UUID

import io.circe.generic.auto._
import io.simplesource.api.CommandError
import io.simplesource.api.CommandError.Reason
import io.simplesource.data.{Result, Sequence}
import io.simplesource.kafka.model._
import org.scalatest.{Matchers, WordSpec}

class JsonCommandTests extends WordSpec with Matchers {
  import TestTypes._
  "io.simplesource.io.simplesource.saga.user.saga.user.command io.simplesource.io.simplesource.saga.user.saga.scala.serdes" must {
    val serdes =
      JsonSerdes.commandSerdes[UUID, UserCommand]
    val topic = "topic"

    "serialise and deserialise io.simplesource.io.simplesource.saga.user.saga.user.command key UUIDs" in {
      val initial = UUID.randomUUID()
      val ser =
        serdes.commandResponseKey().serializer().serialize(topic, initial)
      val de =
        serdes.commandResponseKey().deserializer().deserialize(topic, ser)
      de shouldBe initial
    }

    "serialise and deserialise io.simplesource.io.simplesource.saga.user.saga.user.command requests" in {
      val initial =
        new CommandRequest[UUID, UserCommand](UUID.randomUUID(),
                                              UserCommand.Insert(UUID.randomUUID(), "fn", "ln"),
                                              Sequence.first(),
                                              UUID.randomUUID())
      val ser = serdes.commandRequest().serializer().serialize(topic, initial)
      val de  = serdes.commandRequest().deserializer().deserialize(topic, ser)
      de shouldBe initial
    }

    "serialise and deserialise io.simplesource.io.simplesource.saga.user.saga.user.command response success" in {
      val initial =
        new CommandResponse(UUID.randomUUID(), Sequence.first(), Result.success(Sequence.first().next()))
      val ser = serdes.commandResponse().serializer().serialize(topic, initial)
      val de  = serdes.commandResponse().deserializer().deserialize(topic, ser)
      de shouldBe initial
    }

    "serialise and deserialise io.simplesource.io.simplesource.saga.user.saga.user.command response failure" in {
      val initial =
        new CommandResponse(UUID.randomUUID(),
                            Sequence.first(),
                            Result.failure(CommandError.of(Reason.InvalidCommand, "Invalid io.simplesource.io.simplesource.saga.user.saga.user.command")))
      val ser = serdes.commandResponse().serializer().serialize(topic, initial)
      val de  = serdes.commandResponse().deserializer().deserialize(topic, ser)
      de shouldBe initial
    }
  }
}
