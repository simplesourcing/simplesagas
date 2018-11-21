package topics.serdes

import java.util.UUID

import io.circe.generic.auto._
import io.simplesource.api.CommandError
import io.simplesource.api.CommandError.Reason
import io.simplesource.data.{Result, Sequence}
import io.simplesource.kafka.model._
import org.scalatest.{Matchers, WordSpec}

class JsonAggregateTests extends WordSpec with Matchers {
  import TestTypes._
  "aggregate shared.serdes" must {
    val serdes =
      JsonSerdes.aggregateSerdes[UUID, UserCommand, UserEvent, Option[User]]
    val topic = "topic"

    "serialise and deserialise command key UUIDs" in {
      val initial = UUID.randomUUID()
      val ser =
        serdes.commandResponseKey().serializer().serialize(topic, initial)
      val de =
        serdes.commandResponseKey().deserializer().deserialize(topic, ser)
      de shouldBe initial
    }

    "serialise and deserialise aggregate key UUIDs" in {
      val initial = UUID.randomUUID()
      val ser     = serdes.aggregateKey().serializer().serialize(topic, initial)
      val de      = serdes.aggregateKey().deserializer().deserialize(topic, ser)
      de shouldBe initial
    }

    "serialise and deserialise command requests" in {
      val initial =
        new CommandRequest[UUID, UserCommand](UUID.randomUUID(),
                                              UserCommand.Insert(UUID.randomUUID(), "fn", "ln"),
                                              Sequence.first(),
                                              UUID.randomUUID())
      val ser = serdes.commandRequest().serializer().serialize(topic, initial)
      val de  = serdes.commandRequest().deserializer().deserialize(topic, ser)
      de shouldBe initial
    }

    "serialise and deserialise events" in {
      val initial =
        new ValueWithSequence[UserEvent](UserEvent.Inserted("fn", "ln"), Sequence.first())
      val ser =
        serdes.valueWithSequence().serializer().serialize(topic, initial)
      val de = serdes.valueWithSequence().deserializer().deserialize(topic, ser)
      de shouldBe initial
    }

    "serialise and deserialise aggregate updates" in {
      val initial = new AggregateUpdate[Option[User]](Some(User("fn", "ln", 0)), Sequence.first())
      val ser     = serdes.aggregateUpdate().serializer().serialize(topic, initial)
      val de      = serdes.aggregateUpdate().deserializer().deserialize(topic, ser)
      de shouldBe initial
    }

    "serialise and deserialise aggregate update result" in {
      val update = new AggregateUpdate[Option[User]](Some(User("fn", "ln", 0)), Sequence.first())
      val initial =
        new AggregateUpdateResult[Option[User]](UUID.randomUUID(), Sequence.first(), Result.success(update))
      val ser = serdes.updateResult().serializer().serialize(topic, initial)
      val de  = serdes.updateResult().deserializer().deserialize(topic, ser)
      de shouldBe initial
    }

    "serialise and deserialise command response success" in {
      val initial =
        new CommandResponse(UUID.randomUUID(), Sequence.first(), Result.success(Sequence.first().next()))
      val ser = serdes.commandResponse().serializer().serialize(topic, initial)
      val de  = serdes.commandResponse().deserializer().deserialize(topic, ser)
      de shouldBe initial
    }

    "serialise and deserialise command response failure" in {
      val initial =
        new CommandResponse(UUID.randomUUID(),
                            Sequence.first(),
                            Result.failure(CommandError.of(Reason.InvalidCommand, "Invalid command")))
      val ser = serdes.commandResponse().serializer().serialize(topic, initial)
      val de  = serdes.commandResponse().deserializer().deserialize(topic, ser)
      de shouldBe initial
    }
  }
}
