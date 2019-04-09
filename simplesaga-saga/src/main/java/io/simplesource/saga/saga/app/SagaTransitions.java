package io.simplesource.saga.saga.app;


import io.simplesource.data.Sequence;
import io.simplesource.saga.model.action.ActionCommand;
import io.simplesource.saga.model.action.ActionId;
import io.simplesource.saga.model.action.ActionStatus;
import io.simplesource.saga.model.action.SagaAction;
import io.simplesource.saga.model.messages.SagaStateTransition;
import io.simplesource.saga.model.saga.*;
import lombok.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

final class SagaTransitions {

    @Value(staticConstructor = "of")
    static class SagaWithRetry<A> {
        @Value(staticConstructor = "of")
        static class Retry {
            public final ActionId actionId;
            public final String actionType;
            public final int retryCount;
        }

        public final Saga<A> saga;
        public final List<Retry> retryActions;

        static <A> SagaWithRetry<A> of(Saga<A> saga) {
            return new SagaWithRetry<>(saga, Collections.emptyList());
        }
    }

    @Value(staticConstructor = "of")
    static class ActionUpdate<A> {
        public final SagaAction<A>action;
        public final Optional<ActionCommand<A>> executedCommand;
    }

    private static Logger logger = LoggerFactory.getLogger(SagaTransitions.class);

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

    static <A> SagaWithRetry<A> applyTransition(SagaStateTransition<A> t, Saga<A> s) {
        return t.cata(
                setInitialState -> {
                    Saga<A> i = setInitialState.sagaState;
                    return SagaWithRetry.of(Saga.of(i.sagaId, i.actions, SagaStatus.InProgress, Sequence.first()));
                },
                actionStateChanged -> {
                    SagaAction<A> oa = s.actions.getOrDefault(actionStateChanged.actionId, null);
                    if (oa == null) {
                        logger.error("SagaAction with ID {} could not be found", actionStateChanged.actionId);
                        return SagaWithRetry.of(s);
                    }
                    ActionUpdate<A> actionUpdate = getUpdatedAction(s, actionStateChanged, oa);

                    // TODO: add a MapUtils updated
                    Map<ActionId, SagaAction<A>> actionMap = new HashMap<>();
                    s.actions.forEach((k, existing) -> actionMap.put(k, k.equals(actionStateChanged.actionId) ? actionUpdate.action : existing));

                    List<SagaWithRetry.Retry> retries =
                            actionStateChanged.actionStatus == ActionStatus.RetryAwaiting ?
                                    actionUpdate.executedCommand.map(command ->
                                            Collections.singletonList(
                                                    SagaWithRetry.Retry.of(actionUpdate.action.actionId, command.actionType, oa.retryCount)))
                                            .orElse(Collections.emptyList()) :
                                    Collections.emptyList();

                    return SagaWithRetry.of(s.updated(actionMap, s.status, s.sagaError), retries);
                },

                sagaStatusChanged -> {
                    // TODO: add saga errors as a separate error type
                    return SagaWithRetry.of(s.updated(sagaStatusChanged.sagaStatus, sagaStatusChanged.sagaErrors));
                },

                transitionList -> {
                    Saga<A> sNew = s;
                    List<SagaWithRetry.Retry> retries = new ArrayList<>();
                    for (SagaStateTransition<A> change : transitionList.actions) {
                        SagaWithRetry<A> transResult = applyTransition(change, sNew);
                        sNew = transResult.saga;
                        retries.addAll(transResult.retryActions);
                    }
                    return SagaWithRetry.of(sNew, retries);
                }
        );
    }

    static <A> ActionCommand<A> freshCommand(ActionCommand<A> command) {
        return ActionCommand.of(command.command, command.actionType);
    }

    static <A> ActionUpdate<A> getUpdatedAction(Saga<A> s, SagaStateTransition.SagaActionStateChanged<A> transition, SagaAction<A> oa) {
        ActionStatus newStatus = transition.actionStatus;
        List<SagaError> actionErrors = oa.error;

        boolean inUndo = (s.status == SagaStatus.InFailure || s.status == SagaStatus.Failed);

        ActionCommand<A> aCmd = oa.command;
        Optional<ActionCommand<A>> uCmd = oa.undoCommand;

        if (newStatus == ActionStatus.RetryCompleted) {
            if (s.status == SagaStatus.FailurePending)
                newStatus = ActionStatus.Failed;
            else if (inUndo) {
                newStatus = ActionStatus.Completed;
            }
            else
                newStatus = ActionStatus.Pending;
        }

        if (newStatus == ActionStatus.RetryAwaiting) {
            if (!inUndo) {
                aCmd = freshCommand(oa.command);
                actionErrors = transition.actionErrors;
            }
            else {
                uCmd = uCmd.map(SagaTransitions::freshCommand);
            }
        } else {
            if (!inUndo) {
                Optional<ActionCommand<A>> newUndoCommand = transition.undoCommand.map(uc -> ActionCommand.of(uc.command, uc.actionType));
                uCmd = Optional.ofNullable(newUndoCommand.orElse(oa.undoCommand.orElse(null)));
                if (transition.actionStatus == ActionStatus.Failed) {
                    actionErrors = transition.actionErrors;
                }
            }
        }
        Optional<ActionCommand<A>> eCommand = inUndo ? uCmd : Optional.of(aCmd);

        SagaAction<A> newAction =
                SagaAction.of(
                        oa.actionId,
                        aCmd,
                        uCmd,
                        oa.dependencies,
                        newStatus,
                        actionErrors,
                        updatedRetryCount(oa, newStatus));

        return ActionUpdate.of(newAction, eCommand);
    }

    private static <A> int updatedRetryCount(SagaAction<A> oa, ActionStatus newStatus) {
        switch (newStatus) {
            case Completed:
                return 0;
            case RetryAwaiting:
                return oa.retryCount + 1;
            default:
                return oa.retryCount;
        }
    }
}
