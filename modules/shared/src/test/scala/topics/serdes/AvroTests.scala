package topics.serdes

import java.util.UUID

import com.sksamuel.avro4s.{Record, RecordFormat}
import org.scalatest.{Matchers, WordSpec}

object TestTypes {
  final case class Key[A](a: A)
  final case class User(firstName: String, lastName: String, yearOfBirth: Int)
  sealed trait UserCommand {
    def userId: UUID
  }

  object UserCommand {
    final case class Insert(userId: UUID, firstName: String, lastName: String) extends UserCommand
  }

  sealed trait UserEvent
  object UserEvent {
    final case class Inserted(firstName: String, lastName: String) extends UserEvent
    final case class Deleted()                                     extends UserEvent
  }
}

class AvroTests extends WordSpec with Matchers {
  import TestTypes._

  "record format" must {
    "serialise and deserialise Strings" ignore {
      val recordFormat   = RecordFormat[String]
      val s              = "TestString"
      val record: Record = recordFormat.to(s)
      val sAfter         = recordFormat.from(record)
      sAfter shouldBe s
    }

    "serialise and deserialise uuids" ignore {
      val recordFormat   = RecordFormat[UUID]
      val uuid           = UUID.randomUUID()
      val record: Record = recordFormat.to(uuid)
      val uuidAfter      = recordFormat.from(record)
      uuidAfter shouldBe uuid
    }

    "serialise and deserialise uuids in final case class" in {
      val recordFormat   = RecordFormat[Key[UUID]]
      val key            = Key(UUID.randomUUID())
      val record: Record = recordFormat.to(key)
      val keyAfter       = recordFormat.from(record)
      keyAfter shouldBe key
    }

    "serialise and deserialise users" in {
      val user           = User("Firstname", "Lastname", 1999)
      val recordFormat   = RecordFormat[User]
      val record: Record = recordFormat.to(user)
      val userAfter      = recordFormat.from(record)
      userAfter shouldBe user
    }

    "serialise and deserialise user commands" ignore {
      val insertUser      = UserCommand.Insert(UUID.randomUUID(), "Firstname", "Lastname")
      val recordFormat    = RecordFormat[UserCommand]
      val record: Record  = recordFormat.to(insertUser)
      val insertUserAfter = recordFormat.from(record)
      insertUserAfter shouldBe insertUser
    }
  }
}
