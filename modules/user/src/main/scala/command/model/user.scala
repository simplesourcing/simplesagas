package command.model
import java.util.UUID

object user {
  final case class User(firstName: String, lastName: String, yearOfBirth: Int)

  sealed trait UserCommand {
    def userId: UUID
  }

  object UserCommand {
    final case class Insert(userId: UUID, firstName: String, lastName: String)     extends UserCommand
    final case class UpdateName(userId: UUID, firstName: String, lastName: String) extends UserCommand
    final case class UpdateYearOfBirth(userId: UUID, yearOfBirth: Int)             extends UserCommand
    final case class Delete(userId: UUID)                                          extends UserCommand
  }

  sealed trait UserEvent
  object UserEvent {
    final case class Inserted(firstName: String, lastName: String)    extends UserEvent
    final case class NameUpdated(firstName: String, lastName: String) extends UserEvent
    final case class YearOfBirthUpdated(yearOfBirth: Int)             extends UserEvent
    final case class Deleted()                                        extends UserEvent
  }
}
