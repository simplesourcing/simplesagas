package model

import java.util.UUID

import model.saga._

object messages {
  final case class ActionRequest[A](sagaId: UUID,
                                    actionId: UUID,
                                    actionCommand: ActionCommand[A],
                                    actionType: String)
  final case class ActionResponse(sagaId: UUID,
                                  actionId: UUID,
                                  commandId: UUID,
                                  result: Either[SagaError, Unit])

  final case class SagaRequest[A](sagaId: UUID, initialState: Saga[A])
  final case class SagaResponse(sagaId: UUID, result: Either[SagaError, Unit])

  sealed trait SagaStateTransition[A]
  final case class SetInitialState[A](sagaState: Saga[A]) extends SagaStateTransition[A]
  final case class SagaActionStatusChanged[A](sagaId: UUID, actionId: UUID, actionStatus: ActionStatus)
      extends SagaStateTransition[A]
  final case class SagaStatusChanged[A](sagaId: UUID, sagaStatus: SagaStatus) extends SagaStateTransition[A]
  final case class TransitionList[A](transitionList: List[SagaStateTransition[A]])
      extends SagaStateTransition[A]
}
