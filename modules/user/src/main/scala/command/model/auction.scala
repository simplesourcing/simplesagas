package command.model
import java.util.UUID

object auction {
  final case class Reservation(reservationId: UUID, description: String, amount: BigDecimal)
  final case class Account(name: String, funds: BigDecimal, reservations: List[Reservation] = List.empty)

  sealed trait AccountCommand {
    def accountId: UUID
  }

  object AccountCommand {
    final case class CreateAccount(accountId: UUID, userName: String, funds: BigDecimal)
        extends AccountCommand
    final case class UpdateAccount(accountId: UUID, userName: String) extends AccountCommand
    final case class AddFunds(accountId: UUID, funds: BigDecimal)     extends AccountCommand
    final case class ReserveFunds(accountId: UUID,
                                  reservationId: UUID,
                                  amount: BigDecimal,
                                  description: String)
        extends AccountCommand
    final case class CancelReservation(accountId: UUID, reservationId: UUID) extends AccountCommand
    final case class ConfirmReservation(accountId: UUID, reservationId: UUID, finalAmount: BigDecimal)
        extends AccountCommand
  }

  sealed trait AccountEvent
  object AccountEvent {
    final case class AccountCreated(accountId: UUID, userName: String, initialFunds: BigDecimal)
        extends AccountEvent
    final case class AccountUpdated(accountId: UUID, userName: String) extends AccountEvent
    final case class FundsAdded(accountId: UUID, amount: BigDecimal)   extends AccountEvent
    final case class FundsReserved(accountId: UUID,
                                   reservationId: UUID,
                                   amount: BigDecimal,
                                   description: String)
        extends AccountEvent
    final case class ReservationCancelled(reservationId: UUID)                     extends AccountEvent
    final case class ReservationConfirmed(reservationId: UUID, amount: BigDecimal) extends AccountEvent
  }

  sealed trait AuctionCommand {
    def auctionId: UUID
  }

  final case class Bid(accountId: UUID, amount: BigDecimal)

  sealed trait AuctionStatus
  object AuctionStatus {
    case object Open
    case object Closed
  }

  final case class Auction(bidHistory: List[Bid], auctionStatus: AuctionStatus)

  object AuctionCommand {
    final case class CreateAuction(auctionId: UUID,
                                   itemName: String,
                                   startingBid: BigDecimal,
                                   reserve: BigDecimal)
        extends AuctionCommand
    final case class PlaceBid(auctionId: UUID, accountId: UUID, amount: BigDecimal) extends AuctionCommand
    final case class CloseAuction(auctionId: UUID)
  }
}
