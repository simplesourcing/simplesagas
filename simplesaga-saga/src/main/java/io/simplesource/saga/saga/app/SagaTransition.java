package io.simplesource.saga.saga.app;


import io.simplesource.data.Sequence;
import io.simplesource.saga.model.action.ActionId;
import io.simplesource.saga.model.action.ActionStatus;
import io.simplesource.saga.model.action.SagaAction;
import io.simplesource.saga.model.messages.SagaStateTransition;
import io.simplesource.saga.model.saga.*;

import java.util.*;

final class SagaTransition {

    static <A> ActionTransition.SagaWithRetry<A> applyTransition(SagaStateTransition<A> t, Saga<A> saga) {
        return t.cata(
                setInitialState -> {
                    Saga<A> i = setInitialState.sagaState;
                    return ActionTransition.SagaWithRetry.of(Saga.of(i.sagaId, i.actions, SagaStatus.InProgress, Sequence.first()));
                },
                actionStateChanged -> {
                    SagaAction<A> originalAction = saga.actions.getOrDefault(actionStateChanged.actionId, null);
                    return ActionTransition.withActionStateChanged(saga, actionStateChanged, originalAction);
                },

                sagaStatusChanged -> {
                    Saga<A> updatedSaga = withSagaStatusChanged(saga, sagaStatusChanged);
                    return ActionTransition.SagaWithRetry.of(updatedSaga);
                },

                transitionList -> {
                    Saga<A> sNew = saga;
                    List<ActionTransition.SagaWithRetry.Retry> retries = new ArrayList<>();
                    for (SagaStateTransition<A> change : transitionList.actions) {
                        ActionTransition.SagaWithRetry<A> transResult = applyTransition(change, sNew);
                        sNew = transResult.saga;
                        retries.addAll(transResult.retryActions);
                    }
                    return ActionTransition.SagaWithRetry.of(sNew, retries);
                }
        );
    }

    private static <A> Saga<A> withSagaStatusChanged(Saga<A> saga, SagaStateTransition.SagaStatusChanged<A> sagaStatusChanged) {
        Map<ActionId, SagaAction<A>> sagaActions = saga.actions;
        if ((sagaStatusChanged.sagaStatus == SagaStatus.FailurePending ||
                sagaStatusChanged.sagaStatus == SagaStatus.InFailure ||
                sagaStatusChanged.sagaStatus == SagaStatus.Failed)
                && saga.status == SagaStatus.InProgress) {
            ActionStatuses statuses = ActionStatuses.of(saga);
            if (statuses.has(ActionStatus.RetryAwaiting)) {
                sagaActions = setAwaitingRetryToFailed(saga);
            }
        }

        return saga.updated(sagaActions, sagaStatusChanged.sagaStatus, sagaStatusChanged.sagaErrors);
    }

    private static <A> Map<ActionId, SagaAction<A>> setAwaitingRetryToFailed(Saga<A> saga) {
        Map<ActionId, SagaAction<A>> actions = new HashMap<>();
        saga.actions.forEach((k, v) ->
                actions.put(k, v.status == ActionStatus.RetryAwaiting ? v.updated(ActionStatus.Failed) : v));
        return actions;
    }
}
