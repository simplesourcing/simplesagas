package io.simplesource.saga.saga.internal;

import io.simplesource.api.CommandId;
import io.simplesource.kafka.internal.util.Tuple2;
import io.simplesource.saga.model.action.ActionCommand;
import io.simplesource.saga.model.action.ActionStatus;
import io.simplesource.saga.model.action.SagaAction;
import io.simplesource.saga.model.messages.ActionRequest;
import io.simplesource.saga.model.messages.ActionResponse;
import io.simplesource.saga.model.messages.SagaStateTransition;
import io.simplesource.saga.model.saga.Saga;
import io.simplesource.saga.model.saga.SagaActionExecution;
import io.simplesource.saga.model.saga.SagaId;
import io.simplesource.saga.model.saga.SagaStatus;
import io.simplesource.saga.saga.app.SagaContext;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

final class ActionStream {
    static <A> Tuple2<KStream<SagaId, SagaStateTransition<A>>, KStream<SagaId, ActionRequest<A>>> getNextActions(
            KStream<SagaId, Saga<A>> sagaState) {

        // get the next actions from the state updates
        KStream<SagaId, List<SagaActionExecution<A>>> nextActionsListStream =
                sagaState.mapValues((k, state) -> ActionResolver.getNextActions(state));

        KStream<SagaId, SagaActionExecution<A>> nextActionsStream =
                nextActionsListStream.flatMapValues((k, v) -> v);

        KStream<SagaId, SagaStateTransition<A>> stateUpdateNewActions = nextActionsListStream
                .filter((k, actions) -> !actions.isEmpty())
                .<SagaStateTransition<A>>mapValues((sagaId, actions) -> {
                    List<SagaStateTransition.SagaActionStateChanged<A>> transitions = actions.stream().map(action ->
                            SagaStateTransition.SagaActionStateChanged.<A>of(
                                    sagaId,
                                    action.actionId,
                                    action.status,
                                    Collections.emptyList(),
                                    Optional.empty(),
                                    action.isUndo)
                    ).collect(Collectors.toList());
                    return SagaStateTransition.TransitionList.of(transitions);
                })
                .peek(SagaStream.logValues("stateUpdateNewActions"));

        KStream<SagaId, ActionRequest<A>> actionRequests =
                nextActionsStream
                        .filter((sagaId, v) -> v.command.isPresent())
                        .mapValues((sagaId, ae) ->
                                ActionRequest.of(sagaId,
                                        ae.actionId,
                                        ae.command.get(),
                                        ae.isUndo))
                        .peek(SagaStream.logValues("publishActionRequests"));

        return Tuple2.of(stateUpdateNewActions, actionRequests);
    }

    static <A> KStream<SagaId, SagaStateTransition<A>> handleActionResponses(
            SagaContext<A> ctx,
            KStream<SagaId, ActionResponse<A>> actionResponses,
            KTable<SagaId, Saga<A>> sagaState) {
        KStream<SagaId, ActionResponse<A>>[] successFailure = actionResponses.branch(
                (sId, resp) -> resp.result.isSuccess(),
                (sId, resp) -> resp.result.isFailure());

        KStream<SagaId, ActionResponse<A>> success = successFailure[0];
        KStream<SagaId, ActionResponse<A>> failure = successFailure[1];

        KStream<SagaId, SagaStateTransition<A>> successTransitions = success.mapValues((sagaId, response) ->
                SagaStateTransition.SagaActionStateChanged.of(
                        sagaId,
                        response.actionId,
                        response.isUndo ? ActionStatus.Undone : ActionStatus.Completed,
                        Collections.emptyList(),
                        response.result.getOrElse(Optional.empty()),
                        response.isUndo));

        KStream<SagaId, Tuple2<Optional<Duration>, ActionResponse<A>>> failureWithRetries = failure.join(sagaState, (e, s) -> {
            // if failure is pending, don't retry
            if (s.status == SagaStatus.FailurePending) return Tuple2.of(Optional.empty(), e);
            SagaAction<A> action = s.actions.get(e.actionId);
            CommandId eCid = e.commandId;
            ActionCommand<A> retryAction = eCid.equals(action.command.commandId) ?
                    action.command :
                    action.undoCommand
                            .filter(u -> u.commandId.equals(eCid))
                            .orElse(null);

            Optional<Duration> nextRetry = Optional.ofNullable(retryAction).flatMap(ra ->
                    ctx.retryStrategies.get(ra.actionType).nextRetry(action.retryCount));

            return Tuple2.of(nextRetry, e);
        });

        KStream<SagaId, SagaStateTransition<A>> failureTransitions = failureWithRetries.mapValues(tuple ->  {
            ActionResponse<A> response = tuple.v2();
            boolean retry = tuple.v1().isPresent();
            ActionStatus newStatus = retry ?
                    ActionStatus.RetryAwaiting :
                    (response.isUndo ? ActionStatus.UndoFailed : ActionStatus.Failed);
            return SagaStateTransition.SagaActionStateChanged.of(
                    response.sagaId,
                    response.actionId,
                    newStatus,
                    response.result.failureReasons().get(),
                    Optional.empty(),
                    response.isUndo);
        });

        return successTransitions.merge(failureTransitions).peek(SagaStream.logValues("stateTransitionsActionResponse"));
    }
}
