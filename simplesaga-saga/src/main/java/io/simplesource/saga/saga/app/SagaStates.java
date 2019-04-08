package io.simplesource.saga.saga.app;

import io.simplesource.saga.model.action.ActionStatus;
import io.simplesource.saga.model.saga.Saga;
import io.simplesource.saga.model.saga.SagaStatus;
import lombok.Value;

import java.util.Set;
import java.util.stream.Collectors;

@Value
class SagaStates {
    Set<ActionStatus> actionStates;
    public final SagaStatus sagaStatus;

    private SagaStates(Saga<?> s) {
        actionStates = s.actions.values().stream().map(a -> a.status).collect(Collectors.toSet());
        sagaStatus = s.status;
    }

    static public SagaStates of(Saga<?> s) {
        return new SagaStates(s);
    }

    boolean has(ActionStatus status) {
        return actionStates.contains(status);
    }

    boolean missing(ActionStatus status) {
        return !has(status);
    }

    boolean failurePending() {
        return has(ActionStatus.Failed) &&
                (has(ActionStatus.InProgress) || has(ActionStatus.AwaitingRetry));
    }

    boolean inFailure() {
        return has(ActionStatus.Failed) &&
                missing(ActionStatus.InProgress) &&
                (has(ActionStatus.InUndo) || has(ActionStatus.Completed));
    }

    boolean failed() {
        return has(ActionStatus.Failed) &&
                missing(ActionStatus.InProgress) &&
                missing(ActionStatus.InUndo) &&
                missing(ActionStatus.Completed) &&
                missing(ActionStatus.AwaitingRetry);
    }

    boolean completed() {
        return has(ActionStatus.Completed) && actionStates.size() == 1;
    }
}
