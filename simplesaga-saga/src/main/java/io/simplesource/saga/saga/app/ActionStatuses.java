package io.simplesource.saga.saga.app;

import io.simplesource.saga.model.action.ActionStatus;
import io.simplesource.saga.model.saga.Saga;
import io.simplesource.saga.model.saga.SagaStatus;
import lombok.Value;

import java.util.Set;
import java.util.stream.Collectors;

@Value
class ActionStatuses {
    Set<ActionStatus> actionStates;
    public final SagaStatus sagaStatus;

    private ActionStatuses(Saga<?> s) {
        actionStates = s.actions.values().stream().map(a -> a.status).collect(Collectors.toSet());
        sagaStatus = s.status;
    }

    static public ActionStatuses of(Saga<?> s) {
        return new ActionStatuses(s);
    }

    boolean has(ActionStatus status) {
        return actionStates.contains(status);
    }

    boolean missing(ActionStatus status) {
        return !has(status);
    }

    boolean failurePending() {
        return has(ActionStatus.Failed) &&
                (has(ActionStatus.InProgress) || has(ActionStatus.RetryAwaiting));
    }

    boolean inFailure() {
        return has(ActionStatus.Failed) &&
                missing(ActionStatus.InProgress) &&
                (has(ActionStatus.UndoInProgress) || has(ActionStatus.Completed));
    }

    boolean failed() {
        return has(ActionStatus.Failed) &&
                missing(ActionStatus.InProgress) &&
                missing(ActionStatus.UndoInProgress) &&
                missing(ActionStatus.Completed) &&
                missing(ActionStatus.RetryAwaiting);
    }

    boolean completed() {
        return has(ActionStatus.Completed) && actionStates.size() == 1;
    }
}
