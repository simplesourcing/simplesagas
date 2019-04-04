package io.simplesource.saga.saga.app;


import io.simplesource.data.Sequence;
import io.simplesource.saga.model.action.ActionCommand;
import io.simplesource.saga.model.action.ActionId;
import io.simplesource.saga.model.action.ActionStatus;
import io.simplesource.saga.model.action.SagaAction;
import io.simplesource.saga.model.messages.SagaStateTransition;
import io.simplesource.saga.model.saga.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

final class SagaTransitions {
    private static Logger logger = LoggerFactory.getLogger(SagaTransitions.class);

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

    static <A> boolean actionInProgress(Saga<A> sagaState) {
        return sagaState.actions.values()
                .stream()
                .anyMatch(a -> a.status.equals(ActionStatus.InProgress));
    }

    static <A> boolean sagaFailurePending(Saga<A> sagaState) {
        return failedAction(sagaState) && actionInProgress(sagaState);
    }

    static <A> boolean sagaInFailure(Saga<A> sagaState) {
        return failedAction(sagaState) && !actionInProgress(sagaState) && sagaUndoesPending(sagaState);
    }

    static <A> boolean sagaFailed(Saga<A> sagaState) {
        return failedAction(sagaState) && !actionInProgress(sagaState) && !sagaUndoesPending(sagaState);
    }

    static <A> boolean sagaCompleted(Saga<A> sagaState) {
        for (SagaAction<A> a : sagaState.actions.values()) {
            if (a.status != ActionStatus.Completed)
                return false;
        }
        return true;
    }

    static <A> List<SagaActionExecution<A>> getNextActions(Saga<A> sagaState) {
        if (sagaState.status == SagaStatus.InProgress) {
            Set<ActionId> doneKeys = sagaState.actions
                    .entrySet()
                    .stream()
                    .filter(entry -> entry.getValue().status == ActionStatus.Completed)
                    .map(Map.Entry::getValue)
                    .map(x -> x.actionId)
                    .collect(Collectors.toSet());
            List<SagaActionExecution<A>> pendingActions = sagaState.actions
                    .values()
                    .stream()
                    .filter(action -> action.status == ActionStatus.Pending && doneKeys.containsAll(action.dependencies))
                    .map(a -> SagaActionExecution.of(a.actionId, Optional.of(a.command), ActionStatus.InProgress, false))
                    .collect(Collectors.toList());
            return pendingActions;
        } else if (sagaState.status == SagaStatus.InFailure) {
            // reverse the arrows in the dependency graph
            Map<ActionId, Set<ActionId>> reversed = new HashMap<>();
            sagaState.actions.values().forEach(action -> {
                action.dependencies.forEach(dep -> {
                    reversed.putIfAbsent(dep, new HashSet<>());
                    Set<ActionId> revSet = reversed.get(dep);
                    revSet.add(action.actionId);
                });
            });

            Set<ActionId> undoneKeys = sagaState.actions.entrySet().stream()
                    .filter(entry -> {
                        ActionStatus status = entry.getValue().status;
                        return status != ActionStatus.InUndo && status != ActionStatus.Completed;
                    })
                    .map(Map.Entry::getKey)
                    .collect(Collectors.toSet());

            Map<ActionId, SagaAction<A>> pendingUndoes = sagaState.actions
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
                        return SagaActionExecution.of(a.actionId, a.undoCommand, status, true);

                    })
                    .collect(Collectors.toList());

            return pendingExecutions;
        }
        return Collections.emptyList();
    }

    static <A> Saga<A> applyTransition(SagaStateTransition<A> t, Saga<A> s) {
        return t.cata(
                setInitialState -> {
                    Saga<A> i = setInitialState.sagaState;
                    return Saga.of(i.sagaId, i.actions, SagaStatus.InProgress, Sequence.first());
                },
                actionStateChanged -> {
                    SagaAction<A> oa = s.actions.getOrDefault(actionStateChanged.actionId, null);
                    if (oa == null) {
                        logger.error("SagaAction with ID {} could not be found", actionStateChanged.actionId);
                        return s;
                    }
                    ActionStatus newStatus =  actionStateChanged.actionStatus;
                    if (oa.status == ActionStatus.InUndo) {
                        if (actionStateChanged.actionStatus == ActionStatus.Completed) newStatus = ActionStatus.Undone;
                        else if (actionStateChanged.actionStatus == ActionStatus.Failed) newStatus = ActionStatus.UndoFailed;
                    }

                    Optional<ActionCommand<A>> newUndoCommand = s.status == SagaStatus.InFailure ? Optional.empty() :
                            actionStateChanged.undoCommand.map(uc -> ActionCommand.of(uc.command, uc.actionType));
                    // This mess can replace by 'newUndoCommand.or(oa.undoCommand)' in Java 9+.
                    Optional<ActionCommand<A>> undoCmd = Optional.ofNullable(newUndoCommand.orElse(oa.undoCommand.orElse(null)));

                    SagaAction<A> action =
                            SagaAction.of(oa.actionId, oa.command, undoCmd, oa.dependencies, newStatus, actionStateChanged.actionErrors, oa.retryCount);

                    // TODO: add a MapUtils updated
                    Map<ActionId, SagaAction<A>> actionMap = new HashMap<>();
                    s.actions.forEach((k, v) -> actionMap.put(k, k.equals(actionStateChanged.actionId) ? action : v));
                    return s.updated(actionMap, s.status, s.sagaError);
                },

                sagaStatusChanged -> {
                    // TODO: add saga errors as a separate error type
                    return s.updated(sagaStatusChanged.sagaStatus, sagaStatusChanged.sagaErrors);
                },

                transitionList -> {
                    Saga<A> sNew = s;
                    for (SagaStateTransition<A> change: transitionList.actions) {
                        sNew = applyTransition(change, sNew);
                    }
                    return sNew;
                }
        );

    }
}
