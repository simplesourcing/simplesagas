package command.handlers
import java.util.UUID

import command.model.user.{User, UserCommand, UserEvent}
import io.simplesource.api.CommandError
import io.simplesource.data.{NonEmptyList, Result}

object UserHandlers {
  def aggregator(a: Option[User])(e: UserEvent): Option[User] = e match {
    case UserEvent.Inserted(firstName, lastName) =>
      Option(User(firstName = firstName, lastName = lastName, yearOfBirth = 0))
    case UserEvent.NameUpdated(firstName, lastName) =>
      a.map(_.copy(firstName = firstName, lastName = lastName))
    case UserEvent.YearOfBirthUpdated(yob) => a.map(_.copy(yearOfBirth = yob))
    case UserEvent.Deleted()               => None
  }

  def commandHandler(k: UUID, a: Option[User])(
      c: UserCommand): Result[CommandError, NonEmptyList[UserEvent]] = c match {
    case UserCommand.Insert(_, firstName, lastName) =>
      Result.success(NonEmptyList.of(UserEvent.Inserted(firstName, lastName)))
    case UserCommand.UpdateName(_, firstName, lastName) =>
      Result.success(NonEmptyList.of(UserEvent.NameUpdated(firstName, lastName)))
    case UserCommand.UpdateYearOfBirth(_, yearOfBirth) =>
      Result.success(NonEmptyList.of(UserEvent.YearOfBirthUpdated(yearOfBirth)))
    case UserCommand.Delete(_) =>
      Result.success(NonEmptyList.of(UserEvent.Deleted()))
  }
}
