package io.simplesource.saga.user.command.handlers
import java.util.UUID

import io.simplesource.saga.user.command.model.auction.{Account, AccountCommand, AccountEvent, Reservation}
import io.simplesource.api.CommandError
import io.simplesource.data.{NonEmptyList, Result}

object AccountHandlers {
  def aggregator(aOpt: Option[Account])(e: AccountEvent): Option[Account] =
    (e, aOpt) match {
      case (AccountEvent.AccountCreated(_, name, funds), _) =>
        Option(Account(name, funds))
      case (AccountEvent.AccountUpdated(_, userName), Some(a)) =>
        Option(a.copy(name = userName))
      case (AccountEvent.FundsAdded(_, funds), Some(a)) =>
        Option(a.copy(funds = a.funds + funds))
      case (AccountEvent.FundsReserved(_, reservationId, amount, description), Some(a)) =>
        Option(a.copy(reservations = Reservation(reservationId, description, amount) :: a.reservations))
      case (AccountEvent.ReservationCancelled(reservationId), Some(a)) =>
        Option(a.copy(reservations = a.reservations.filter(_.reservationId != reservationId)))
      case (AccountEvent.ReservationConfirmed(reservationId, finalAmount), Some(a)) =>
        Option(
          a.copy(reservations = a.reservations.filter(_.reservationId != reservationId),
                 funds = a.funds - finalAmount))
      case (_, None) => None
    }

  // Crude command handler implementation
  def commandHandler(k: UUID, aOpt: Option[Account])(
      c: AccountCommand): Result[CommandError, NonEmptyList[AccountEvent]] =
    (c, aOpt) match {
      case (AccountCommand.CreateAccount(accountId, name, funds), _) =>
        Result.success(NonEmptyList.of(AccountEvent.AccountCreated(accountId, name, funds)))
      case (AccountCommand.UpdateAccount(accountId, name), Some(_)) =>
        Result.success(NonEmptyList.of(AccountEvent.AccountUpdated(accountId, name)))
      case (AccountCommand.AddFunds(accountId, funds), Some(a)) =>
        Result.success(NonEmptyList.of(AccountEvent.FundsAdded(accountId, funds)))

      case (AccountCommand
              .ReserveFunds(accountId, reservationId, amount, description),
            Some(a)) =>
        val exists = a.reservations.exists(_.reservationId == reservationId)
        if (exists)
          Result.failure(
            NonEmptyList.of(
              CommandError.of(CommandError.Reason.InvalidCommand, "Reservation already exists.")))
        else {
          val sufficient = a.funds - a.reservations.map(_.amount).sum >= amount
          if (sufficient)
            Result.success(
              NonEmptyList.of(AccountEvent
                .FundsReserved(accountId, reservationId, amount, description)))
          else
            Result.failure(
              NonEmptyList.of(CommandError.of(CommandError.Reason.InvalidCommand, "Insufficient funds.")))
        }

      case (AccountCommand.CancelReservation(_, reservationId), Some(a)) =>
        if (a.reservations.exists(_.reservationId == reservationId))
          Result.success(NonEmptyList.of(AccountEvent.ReservationCancelled(reservationId)))
        else
          Result.failure(
            NonEmptyList.of(CommandError.of(CommandError.Reason.InvalidCommand, "Reservation not found.")))

      case (AccountCommand.ConfirmReservation(_, reservationId, finalAmount), Some(a)) =>
        val exists = a.reservations.exists(_.reservationId == reservationId)
        if (exists) {
          val sufficient = a.funds - a.reservations
            .map(_.amount)
            .sum + a.reservations
            .find(_.reservationId == reservationId)
            .get
            .amount >= finalAmount
          if (sufficient)
            Result.success(NonEmptyList.of(AccountEvent.ReservationConfirmed(reservationId, finalAmount)))
          else
            Result.failure(
              NonEmptyList.of(CommandError.of(CommandError.Reason.InvalidCommand, "Insufficient funds.")))
        } else
          Result.failure(
            NonEmptyList.of(CommandError.of(CommandError.Reason.InvalidCommand, "Reservation not found.")))

      case (_, None) =>
        Result.failure(
          NonEmptyList.of(CommandError.of(CommandError.Reason.InvalidCommand, "Account does not exist.")))
    }
}
