package io.simplesource.saga.saga.app;

import io.simplesource.saga.model.action.ActionCommand;
import io.simplesource.saga.model.action.ActionId;
import io.simplesource.saga.model.action.ActionStatus;
import io.simplesource.saga.model.action.SagaAction;
import io.simplesource.saga.model.messages.SagaStateTransition;
import io.simplesource.saga.model.saga.Saga;
import io.simplesource.saga.model.saga.SagaError;
import io.simplesource.saga.model.saga.SagaStatus;
import lombok.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

class ActionTransition {
    private static Logger logger = LoggerFactory.getLogger(ActionTransition.class);

    @Value(staticConstructor = "of")
    static class SagaWithRetry<A> {
        @Value(staticConstructor = "of")
        static class Retry {
            public final ActionId actionId;
            public final String actionType;
            public final int retryCount;
            public final boolean isUndo;
        }

        public final Saga<A> saga;
        public final List<Retry> retryActions;

        static <A> SagaWithRetry<A> of(Saga<A> saga) {
            return new SagaWithRetry<>(saga, Collections.emptyList());
        }
    }

    @Value(staticConstructor = "of")
    private static class ActionUpdate<A> {
        public final SagaAction<A>action;
        public final Optional<ActionCommand<A>> executedCommand;
    }

    static <A> SagaWithRetry<A> withActionStateChanged(Saga<A> s, SagaStateTransition.SagaActionStateChanged<A> actionStateChanged, SagaAction<A> oa) {
        if (oa == null) {
            logger.error("SagaAction with ID {} could not be found", actionStateChanged.actionId);
            return SagaWithRetry.of(s);
        }
        return getActionUpdate(s, actionStateChanged, oa).map(actionUpdate -> {

            // TODO: add a MapUtils updated
            Map<ActionId, SagaAction<A>> actionMap = new HashMap<>();
            s.actions.forEach((k, existing) -> actionMap.put(k, k.equals(actionStateChanged.actionId) ? actionUpdate.action : existing));

            List<SagaWithRetry.Retry> retries =
                    actionStateChanged.actionStatus == ActionStatus.RetryAwaiting ?
                            actionUpdate.executedCommand.map(command ->
                                    Collections.singletonList(
                                            SagaWithRetry.Retry.of(actionUpdate.action.actionId, command.actionType, oa.retryCount, actionStateChanged.isUndo)))
                                    .orElse(Collections.emptyList()) :
                            Collections.emptyList();

            return SagaWithRetry.of(s.updated(actionMap, s.status, s.sagaError), retries);
        }).orElseGet(() ->
                SagaWithRetry.of(s, Collections.emptyList()));
    }

    private static <A> Optional<ActionUpdate<A>> getActionUpdate(Saga<A> s, SagaStateTransition.SagaActionStateChanged<A> transition, SagaAction<A> oa) {
        ActionStatus newStatus = transition.actionStatus;
        List<SagaError> actionErrors = oa.error;

        // boolean isUndo = (s.status == SagaStatus.InFailure || s.status == SagaStatus.Failed);
        boolean isUndo = transition.isUndo;

        ActionCommand<A> aCmd = oa.command;
        Optional<ActionCommand<A>> uCmd = oa.undoCommand;

        if (newStatus == ActionStatus.RetryCompleted) {
            if (s.status == SagaStatus.FailurePending)
                newStatus = ActionStatus.Failed;
            else if (isUndo) {
                if (oa.status == ActionStatus.UndoFailed)
                    return Optional.empty();
                newStatus = ActionStatus.Completed;
            }
            else {
                if (oa.status == ActionStatus.Failed)
                    return Optional.empty();
                newStatus = ActionStatus.Pending;
            }
        } else if (newStatus == ActionStatus.RetryAwaiting) {
            if (isUndo) {
                uCmd = uCmd.map(ActionTransition::freshCommand);
            } else {
                aCmd = freshCommand(oa.command);
                actionErrors = transition.actionErrors;
            }
        } else {
            if (!isUndo) {
                Optional<ActionCommand<A>> newUndoCommand = transition.undoCommand.map(uc -> ActionCommand.of(uc.command, uc.actionType));
                uCmd = Optional.ofNullable(newUndoCommand.orElse(oa.undoCommand.orElse(null)));
                if (transition.actionStatus == ActionStatus.Failed) {
                    actionErrors = transition.actionErrors;
                }
            }
        }
        Optional<ActionCommand<A>> eCommand = isUndo ? uCmd : Optional.of(aCmd);

        SagaAction<A> newAction =
                SagaAction.of(
                        oa.actionId,
                        aCmd,
                        uCmd,
                        oa.dependencies,
                        newStatus,
                        actionErrors,
                        updatedRetryCount(oa, newStatus));

        return Optional.of(ActionUpdate.of(newAction, eCommand));
    }

    private static <A> ActionCommand<A> freshCommand(ActionCommand<A> command) {
        return ActionCommand.of(command.command, command.actionType);
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
