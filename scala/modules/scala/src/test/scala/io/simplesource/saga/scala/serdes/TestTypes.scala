package io.simplesource.saga.scala.serdes

import java.util.UUID

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
