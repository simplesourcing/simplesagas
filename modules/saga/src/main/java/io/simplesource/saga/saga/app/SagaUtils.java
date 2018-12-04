package io.simplesource.saga.saga.app;


import io.simplesource.saga.model.saga.ActionStatus;
import io.simplesource.saga.model.saga.Saga;
import io.simplesource.saga.model.saga.SagaAction;

public final class SagaUtils {

  private static <A> boolean sagaUndoesPending(Saga<A> sagaState) {
      //  TODO better exists
      for (SagaAction<A> a: sagaState.actions.values()) {
          if (a.status == ActionStatus.Completed || a.status == ActionStatus.InUndo)
              return true;
      }
      return false;
  }

    private static <A> boolean failedAction(Saga<A> sagaState) {
        for (SagaAction<A> a: sagaState.actions.values()) {
            if (a.status == ActionStatus.Failed)
                return true;
        }
        return false;
    }

    private static <A> boolean sagaInFailure(Saga<A> sagaState) {
        return failedAction(sagaState) && sagaUndoesPending(sagaState);
    }

    private static <A> boolean sagaFailed(Saga<A> sagaState) {
        return failedAction(sagaState) && !sagaUndoesPending(sagaState);
    }



//  def sagaInFailure<A>(sagaState: Saga<A>) =
//    failedAction(sagaState) && sagaUndoesPending<A>(sagaState)
//
//  def sagaFailed<A>(sagaState: Saga<A>) =
//    failedAction(sagaState) && !sagaUndoesPending<A>(sagaState)
//
//  def sagaCompleted<A>(sagaState: Saga<A>) =
//    sagaState.actions.values.forall(_.status == ActionStatus.Completed)
//
//  def getNextActions<A>(sagaState: Saga<A>): List<SagaActionExecution<A>> = sagaState.status match {
//    case SagaStatus.InProgress =>
//      // all actions that have already been completed
//      val doneKeys =
//        sagaState.actions.filter { case (_, action) => action.status == ActionStatus.Completed }.keySet
//      val pendingActions: Map<UUID, SagaAction<A>> = sagaState.actions
//        .filter {
//          case (_, action) =>
//            action.status == ActionStatus.Pending && action.dependencies.subsetOf(doneKeys)
//        }
//      pendingActions.values
//        .map(a => SagaActionExecution<A>(a.actionId, a.actionType, Some(a.command), ActionStatus.InProgress))
//        .toList
//
//    case SagaStatus.InFailure =>
//      // Don't start undo process until all in progress actions are complete
//      val actionsInProgress = sagaState.actions.exists {
//        case (_, action) => action.status == ActionStatus.InProgress
//      }
//      if (actionsInProgress) List.empty
//      else {
//
//        // reverse the arrows in the dependency graph
//        val reverseDependencies: Map<UUID, Set<UUID>> = sagaState.actions.values
//          .flatMap(action => action.dependencies.toSeq.map(depId => (depId, action.actionId)))
//          .groupBy(_._1)
//          .mapValues(_.map(_._2).toSet)
//          .withDefaultValue(Set.empty<UUID>)
//
//        // All actions that do not need to be undone, or are not in the process of being undone
//        val undoneKeys = sagaState.actions.filter {
//          case (_, action) => action.status != ActionStatus.InUndo && action.status != ActionStatus.Completed
//        }.keySet
//
//        // any actions that are completed, and none of the actions that depend on them still need to be undone
//        val pendingUndoes: Map<UUID, SagaAction<A>> = sagaState.actions
//          .filter {
//            case (_, action) =>
//              action.status == ActionStatus.Completed && reverseDependencies(action.actionId)
//                .subsetOf(undoneKeys)
//          }
//        val pendingExecutions = pendingUndoes.values
//          .map(a => {
//            val status =
//              a.undoCommand.fold<ActionStatus>(ActionStatus.UndoBypassed)(_ => ActionStatus.InUndo)
//            SagaActionExecution<A>(a.actionId, a.actionType, a.undoCommand, status)
//          })
//          .toList
//        pendingExecutions
//      }
//
//    case _ => List.empty
//  }
//
//  def applyTransition<A>(t: SagaStateTransition<A>, s: Saga<A>): Saga<A> = {
//    val newState = t match {
//      case SetInitialState(initialState) => initialState.copy(status = SagaStatus.InProgress)
//      case SagaActionStatusChanged(_, actionId, transitionStatus) =>
//        val oldAction = s.actions(actionId)
//        val newStatus = (oldAction.status, transitionStatus) match {
//          case (ActionStatus.InUndo, ActionStatus.Completed)     => ActionStatus.Undone
//          case (ActionStatus.InUndo, ActionStatus.Failed(error)) => ActionStatus.UndoFailed(error)
//          case _                                                 => transitionStatus
//        }
//        val action   = oldAction.copy(status = newStatus)
//        val newState = s.copy(actions = s.actions.updated(action.actionId, action))
//        newState
//
//      case SagaStatusChanged(_, newStatus) => s.copy(status = newStatus)
//      case TransitionList(list)            => list.foldLeft(s)((nextS, t) => applyTransition(t, nextS))
//    }
//    newState.copy(sequence = s.sequence + 1)
//  }
}


//
//import java.util.UUID
//
//import model.messages._
//import model.saga._
//
//object SagaUtils {
//  private def sagaUndoesPending<A>(sagaState: Saga<A>) =
//    sagaState.actions.values.exists(a =>
//      a.status == ActionStatus.Completed || a.status == ActionStatus.InUndo)
//
//  private def failedAction<A>(sagaState: Saga<A>) =
//    sagaState.actions.values
//      .map(_.status)
//      .collectFirst { case ActionStatus.Failed(_) => true }
//      .nonEmpty
//
//  def sagaInFailure<A>(sagaState: Saga<A>) =
//    failedAction(sagaState) && sagaUndoesPending<A>(sagaState)
//
//  def sagaFailed<A>(sagaState: Saga<A>) =
//    failedAction(sagaState) && !sagaUndoesPending<A>(sagaState)
//
//  def sagaCompleted<A>(sagaState: Saga<A>) =
//    sagaState.actions.values.forall(_.status == ActionStatus.Completed)
//
//  def getNextActions<A>(sagaState: Saga<A>): List<SagaActionExecution<A>> = sagaState.status match {
//    case SagaStatus.InProgress =>
//      // all actions that have already been completed
//      val doneKeys =
//        sagaState.actions.filter { case (_, action) => action.status == ActionStatus.Completed }.keySet
//      val pendingActions: Map<UUID, SagaAction<A>> = sagaState.actions
//        .filter {
//          case (_, action) =>
//            action.status == ActionStatus.Pending && action.dependencies.subsetOf(doneKeys)
//        }
//      pendingActions.values
//        .map(a => SagaActionExecution<A>(a.actionId, a.actionType, Some(a.command), ActionStatus.InProgress))
//        .toList
//
//    case SagaStatus.InFailure =>
//      // Don't start undo process until all in progress actions are complete
//      val actionsInProgress = sagaState.actions.exists {
//        case (_, action) => action.status == ActionStatus.InProgress
//      }
//      if (actionsInProgress) List.empty
//      else {
//
//        // reverse the arrows in the dependency graph
//        val reverseDependencies: Map<UUID, Set<UUID>> = sagaState.actions.values
//          .flatMap(action => action.dependencies.toSeq.map(depId => (depId, action.actionId)))
//          .groupBy(_._1)
//          .mapValues(_.map(_._2).toSet)
//          .withDefaultValue(Set.empty<UUID>)
//
//        // All actions that do not need to be undone, or are not in the process of being undone
//        val undoneKeys = sagaState.actions.filter {
//          case (_, action) => action.status != ActionStatus.InUndo && action.status != ActionStatus.Completed
//        }.keySet
//
//        // any actions that are completed, and none of the actions that depend on them still need to be undone
//        val pendingUndoes: Map<UUID, SagaAction<A>> = sagaState.actions
//          .filter {
//            case (_, action) =>
//              action.status == ActionStatus.Completed && reverseDependencies(action.actionId)
//                .subsetOf(undoneKeys)
//          }
//        val pendingExecutions = pendingUndoes.values
//          .map(a => {
//            val status =
//              a.undoCommand.fold<ActionStatus>(ActionStatus.UndoBypassed)(_ => ActionStatus.InUndo)
//            SagaActionExecution<A>(a.actionId, a.actionType, a.undoCommand, status)
//          })
//          .toList
//        pendingExecutions
//      }
//
//    case _ => List.empty
//  }
//
//  def applyTransition<A>(t: SagaStateTransition<A>, s: Saga<A>): Saga<A> = {
//    val newState = t match {
//      case SetInitialState(initialState) => initialState.copy(status = SagaStatus.InProgress)
//      case SagaActionStatusChanged(_, actionId, transitionStatus) =>
//        val oldAction = s.actions(actionId)
//        val newStatus = (oldAction.status, transitionStatus) match {
//          case (ActionStatus.InUndo, ActionStatus.Completed)     => ActionStatus.Undone
//          case (ActionStatus.InUndo, ActionStatus.Failed(error)) => ActionStatus.UndoFailed(error)
//          case _                                                 => transitionStatus
//        }
//        val action   = oldAction.copy(status = newStatus)
//        val newState = s.copy(actions = s.actions.updated(action.actionId, action))
//        newState
//
//      case SagaStatusChanged(_, newStatus) => s.copy(status = newStatus)
//      case TransitionList(list)            => list.foldLeft(s)((nextS, t) => applyTransition(t, nextS))
//    }
//    newState.copy(sequence = s.sequence + 1)
//  }
//}
