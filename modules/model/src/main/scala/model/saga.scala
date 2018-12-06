package model
import java.util.UUID

import cats.data.NonEmptyList

object saga {
  final case class SagaError(messages: NonEmptyList[String])
  object SagaError {
    def of(error: String, tail: String*): SagaError = SagaError(NonEmptyList.of(error, tail: _*))
  }

  sealed trait ActionStatus
  object ActionStatus {
    case object Pending                           extends ActionStatus
    case object InProgress                        extends ActionStatus
    case object Completed                         extends ActionStatus
    final case class Failed(error: SagaError)     extends ActionStatus
    case object InUndo                            extends ActionStatus
    case object Undone                            extends ActionStatus
    case object UndoBypassed                      extends ActionStatus
    final case class UndoFailed(error: SagaError) extends ActionStatus
  }

  sealed trait SagaStatus extends Product with Serializable
  object SagaStatus {
    case object NotStarted                    extends SagaStatus
    case object InProgress                    extends SagaStatus
    case object Completed                     extends SagaStatus
    case object InFailure                     extends SagaStatus
    final case class Failed(error: SagaError) extends SagaStatus
  }

  final case class ActionCommand[A](commandId: UUID, command: A)

  final case class SagaAction[A](actionId: UUID,
                                 actionType: String,
                                 command: ActionCommand[A],
                                 undoCommand: Option[ActionCommand[A]],
                                 dependencies: Set[UUID],
                                 status: ActionStatus)

  final case class SagaActionExecution[A](actionId: UUID,
                                          actionType: String,
                                          command: Option[ActionCommand[A]],
                                          status: ActionStatus)

  final case class Saga[A](actions: Map[UUID, SagaAction[A]], status: SagaStatus, sequence: Int)
}
