package io.simplesource.saga.saga.app;

import io.simplesource.saga.model.action.ActionId;
import io.simplesource.saga.model.action.ActionStatus;
import io.simplesource.saga.model.action.SagaAction;
import io.simplesource.saga.model.saga.Saga;
import io.simplesource.saga.model.saga.SagaActionExecution;
import io.simplesource.saga.model.saga.SagaStatus;

import java.util.*;
import java.util.stream.Collectors;

class ActionResolver {
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
                    .map(a -> SagaActionExecution.of(
                            a.actionId,
                            Optional.of(a.command),
                            ActionStatus.InProgress,
                            false))
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
}
