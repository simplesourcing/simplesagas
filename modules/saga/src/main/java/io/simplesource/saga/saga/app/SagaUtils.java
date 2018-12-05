package io.simplesource.saga.saga.app;


import io.simplesource.data.Sequence;
import io.simplesource.kafka.internal.util.Tuple2;
import io.simplesource.saga.model.messages.SagaStateTransition;
import io.simplesource.saga.model.saga.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

public final class SagaUtils {
    static Logger logger = LoggerFactory.getLogger(SagaUtils.class);

    private static <A> boolean sagaUndoesPending(Saga<A> sagaState) {
        return sagaState.actions.values()
                .stream()
                .map(a -> a.status)
                .anyMatch(s -> s == ActionStatus.Completed || s == ActionStatus.InUndo);
    }

    static <A> boolean failedAction(Saga<A> sagaState) {
        return sagaState.actions.values()
                .stream()
                .anyMatch(a -> a.status.equals(ActionStatus.Failed));
    }

    static <A> boolean sagaInFailure(Saga<A> sagaState) {
        return failedAction(sagaState) && sagaUndoesPending(sagaState);
    }

    static <A> boolean sagaFailed(Saga<A> sagaState) {
        return failedAction(sagaState) && !sagaUndoesPending(sagaState);
    }

    static <A> boolean sagaCompleted(Saga<A> sagaState) {
        for (SagaAction<A> a : sagaState.actions.values()) {
            if (a.status != ActionStatus.Completed)
                return false;
        }
        return true;
    }

    private static <A> List<SagaActionExecution<A>> getNextActions(Saga<A> sagaState) {
        if (sagaState.status == SagaStatus.InProgress) {
            Set<SagaAction<A>> doneKeys = sagaState.actions
                    .entrySet()
                    .stream()
                    .filter(entry -> entry.getValue().status == ActionStatus.Completed)
                    .map(Map.Entry::getValue)
                    .collect(Collectors.toSet());
            List<SagaActionExecution<A>> pendingActions = sagaState.actions
                    .values()
                    .stream()
                    .filter(action -> action.status == ActionStatus.Pending && doneKeys.containsAll(action.dependencies))
                    .map(a -> new SagaActionExecution<A>(a.actionId, a.actionType, Optional.of(a.command), ActionStatus.InProgress))
                    .collect(Collectors.toList());
            return pendingActions;
        } else if (sagaState.status == SagaStatus.InFailure) {
            // reverse the arrows in the dependency graph
            Map<UUID, Set<UUID>> reversed = new HashMap<>();
            sagaState.actions.values().forEach(action -> {
                action.dependencies.forEach(dep -> {
                    reversed.putIfAbsent(dep, new HashSet<>());
                    Set<UUID> revSet = reversed.get(dep);
                    revSet.add(action.actionId);
                });
            });

            Set<UUID> undoneKeys = sagaState.actions.entrySet().stream()
                    .filter(entry -> {
                        ActionStatus status = entry.getValue().status;
                        return status != ActionStatus.InUndo && status != ActionStatus.Completed;
                    })
                    .map(Map.Entry::getKey)
                    .collect(Collectors.toSet());

            Map<UUID, SagaAction<A>> pendingUndoes = sagaState.actions
                    .entrySet()
                    .stream()
                    .filter(entry -> {
                        SagaAction<A> action = entry.getValue();
                        return action.status == ActionStatus.Completed &&
                                undoneKeys.containsAll(reversed.getOrDefault(action.actionId, new HashSet<>()));
                    })
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

            List<SagaActionExecution<A>> pendingExecutions = pendingUndoes.values()
                    .stream()
                    .map(a -> {
                        ActionStatus status = a.undoCommand.map(c -> ActionStatus.InUndo).orElse(ActionStatus.UndoBypassed);
                        return new SagaActionExecution<>(a.actionId, a.actionType, a.undoCommand, status);

                    })
                    .collect(Collectors.toList());

            return pendingExecutions;
        }
        return new ArrayList<>();
    }

    static public <A> Saga<A> applyTransition(SagaStateTransition<A> t, Saga<A> s) {
        if (t instanceof SagaStateTransition.SetInitialState) {
            Saga<A> i = ((SagaStateTransition.SetInitialState<A>) t).sagaState;
            return new Saga<>(i.sagaId, i.actions, SagaStatus.InProgress, Sequence.first());
        }
        if (t instanceof SagaStateTransition.SagaActionStatusChanged) {
            SagaStateTransition.SagaActionStatusChanged<A> st = ((SagaStateTransition.SagaActionStatusChanged<A>) t);
            SagaAction<A> oa = s.actions.getOrDefault(st.actionId, null);
            if (oa == null) {
                logger.error("SagaAction with ID {} could not be found", st.actionId);
                return s;
            }
            ActionStatus newStatus =  st.actionStatus;
            if (oa.status == ActionStatus.InUndo) {
                if (st.actionStatus == ActionStatus.Completed) newStatus = ActionStatus.Undone;
                else if (st.actionStatus == ActionStatus.Failed) newStatus = ActionStatus.UndoFailed;
            }
            SagaAction<A> action =
                    new SagaAction<A>(oa.actionId, oa.actionType, oa.command, oa.undoCommand, oa.dependencies, newStatus, st.actionError);

            // TODO: add a MapUtils updated
            Map<UUID, SagaAction<A>> actionMap = new HashMap<>();
            s.actions.forEach((k, v) -> actionMap.put(k, (k == st.actionId) ? action : v));
            return new Saga<>(s.sagaId, actionMap, s.status, s.sequence.next());
        }
        return s;
    }


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
