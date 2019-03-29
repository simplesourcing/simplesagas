package io.simplesource.saga.saga.app;

import io.simplesource.data.NonEmptyList;
import io.simplesource.data.Result;
import io.simplesource.data.Sequence;
import io.simplesource.kafka.internal.util.Tuple2;
import io.simplesource.saga.model.action.ActionStatus;
import io.simplesource.saga.model.messages.*;
import io.simplesource.saga.model.saga.*;
import io.simplesource.saga.model.serdes.SagaSerdes;
import lombok.Value;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

final public class SagaStream {
    private static Logger logger = LoggerFactory.getLogger(SagaStream.class);

    static <K, V> ForeachAction<K, V> logValues(String prefix) {
        return (k, v) -> logger.info("{}: {}={}", prefix, k.toString().substring(0, 6), v.toString());
    }

    static <A> void addSubTopology(SagaContext<A> ctx,
                                   KStream<SagaId, SagaRequest<A>> sagaRequestStream,
                                   KStream<SagaId, SagaStateTransition> stateTransitionStream,
                                   KStream<SagaId, Saga<A>> stateStream,
                                   KStream<SagaId, ActionResponse> actionResponseStream) {

        // create the state table from the state stream
        KTable<SagaId, Saga<A>> stateTable = createStateTable(ctx, stateStream);

        // validate saga requests
        Tuple2<KStream<SagaId, SagaRequest<A>>, KStream<SagaId, SagaResponse>> vrIr =
                validateSagaRequests(ctx, sagaRequestStream);

        // add stream transformations
        KStream<SagaId, SagaStateTransition> inputStateTransitions = addInitialState(ctx, vrIr.v1(), stateTable);
        Tuple2<KStream<SagaId, SagaStateTransition>, KStream<SagaId, ActionRequest<A>>> rtAr = addNextActions(stateStream);
        KStream<SagaId, SagaStateTransition> responseTransitions = addActionResponses(actionResponseStream);
        Tuple2<KStream<SagaId, SagaStateTransition>, KStream<SagaId, SagaResponse>> stSr = addSagaResponse(stateStream);
        KStream<SagaId, Saga<A>> sagaState = applyStateTransitions(ctx, stateTransitionStream);

        // publish to all the value topics
        SagaProducer.publishActionRequests(ctx, rtAr.v2());
        SagaProducer.publishSagaStateTransitions(ctx, inputStateTransitions);
        SagaProducer.publishSagaStateTransitions(ctx, rtAr.v1());
        SagaProducer.publishSagaStateTransitions(ctx, responseTransitions);
        SagaProducer.publishSagaStateTransitions(ctx, stSr.v1());
        SagaProducer.publishSagaState(ctx, sagaState);
        SagaProducer.publishSagaResponses(ctx, vrIr.v2());
        SagaProducer.publishSagaResponses(ctx, stSr.v2());
    }

    static <A> KTable<SagaId, Saga<A>> createStateTable(SagaContext<A> ctx, KStream<SagaId, Saga<A>> stateStream) {
        return stateStream.groupByKey(Grouped.with(ctx.sSerdes.sagaId(), ctx.sSerdes.state())).reduce(
                (s1, s2) -> (s1.sequence.getSeq() > s2.sequence.getSeq()) ? s1 : s2,
                Materialized.with(ctx.sSerdes.sagaId(), ctx.sSerdes.state()));
    }

    static <A> KStream<SagaId, Saga<A>> applyStateTransitions(SagaContext<A> ctx,
                                                              KStream<SagaId, SagaStateTransition> stateTransitionStream) {
        SagaSerdes<A> sSerdes = ctx.sSerdes;
        return stateTransitionStream
                .groupByKey(Grouped.with(sSerdes.sagaId(), sSerdes.transition()))
                .aggregate(() -> Saga.of(SagaId.random(), new HashMap<>(), SagaStatus.NotStarted, Sequence.first()),
                        (k, t, s) -> SagaUtils.applyTransition(t, s),
                        Materialized.with(sSerdes.sagaId(), sSerdes.state()))
                .toStream();
    }

    private static <A> Tuple2<KStream<SagaId, SagaRequest<A>>, KStream<SagaId, SagaResponse>> validateSagaRequests(
            SagaContext<A> ctx,
            KStream<SagaId, SagaRequest<A>> sagaRequestStream) {

        Set<String> actionTypes = ctx.actionTopicNamers.keySet();

        // check that all action types are valid (may add other validations at some point)
        KStream<SagaId, Tuple2<SagaRequest<A>, List<SagaError>>> y = sagaRequestStream.mapValues((id, request) -> {
            List<SagaError> errors = request.initialState.actions.values().stream().map(action -> {
                String at = action.actionType.toLowerCase();
                SagaError error = !actionTypes.contains(at) ? SagaError.of(SagaError.Reason.InvalidSaga, String.format("Unknown action type '%s'", at)) : null;
                return error;
            }).filter(Objects::nonNull).collect(Collectors.toList());
            return Tuple2.of(request, errors);
        });

        KStream<SagaId, Tuple2<SagaRequest<A>, List<SagaError>>>[] z = y.branch((k, reqWithErrors) -> reqWithErrors.v2().isEmpty(), (k, reqWithErrors) -> !reqWithErrors.v2().isEmpty());
        KStream<SagaId, SagaRequest<A>> validRequests = z[0].mapValues(Tuple2::v1);
        KStream<SagaId, SagaResponse> inValidResponses = z[1].mapValues(Tuple2::v2).mapValues((k, v) -> SagaResponse.of(k, Result.failure(NonEmptyList.fromList(v).get())));

        return Tuple2.of(validRequests, inValidResponses);
    }

    private static <A> KStream<SagaId, SagaStateTransition> addInitialState(SagaContext<A> ctx,
                                                                            KStream<SagaId, SagaRequest<A>> sagaRequestStream,
                                                                            KTable<SagaId, Saga<A>> stateTable) {
        SagaSerdes<A> sSerdes = ctx.sSerdes;
        KStream<SagaId, Tuple2<SagaRequest<A>, Boolean>> newRequestStream = sagaRequestStream.leftJoin(
                stateTable,
                (v1, v2) -> Tuple2.of(v1, v2 == null),
                Joined.with(sSerdes.sagaId(), sSerdes.request(), sSerdes.state()))
                .filter((k, tuple) -> tuple.v2());

        return newRequestStream.mapValues((k, v) -> SagaStateTransition.SetInitialState.of(v.v1().initialState));
    }

    @Value
    private static final class StatusWithError {
        Sequence sequence;
        SagaStatus status;
        Optional<NonEmptyList<SagaError>> errors;

        static Optional<StatusWithError> of(Sequence sequence, SagaStatus status) {
            return Optional.of(new StatusWithError(sequence, status, Optional.empty()));
        }

        static Optional<StatusWithError> of(Sequence sequence, List<SagaError> error) {
            return Optional.of(new StatusWithError(sequence, SagaStatus.Failed, NonEmptyList.fromList(error)));
        }
    }

    static <A> Tuple2<KStream<SagaId, SagaStateTransition>, KStream<SagaId, SagaResponse>> addSagaResponse(KStream<SagaId, Saga<A>> sagaState) {
        KStream<SagaId, StatusWithError> statusWithError = sagaState
                .mapValues((k, state) -> {
                    if (state.status == SagaStatus.InProgress && SagaUtils.sagaCompleted(state))
                        return StatusWithError.of(state.sequence, SagaStatus.Completed);
                    if (state.status == SagaStatus.InProgress && SagaUtils.sagaFailurePending(state))
                        return StatusWithError.of(state.sequence, SagaStatus.FailurePending);
                    if ((state.status == SagaStatus.InProgress || state.status == SagaStatus.FailurePending) &&
                            SagaUtils.sagaInFailure(state))
                        return StatusWithError.of(state.sequence, SagaStatus.InFailure);
                    if ((state.status == SagaStatus.InFailure || state.status == SagaStatus.InProgress) &&
                            SagaUtils.sagaFailed(state)) {
                        List<SagaError> errors = state.actions.values().stream()
                                .filter(action -> action.status == ActionStatus.Failed && !action.error.isEmpty())
                                .flatMap(action -> action.error.stream())
                                .collect(Collectors.toList());

                        return StatusWithError.of(state.sequence, errors);
                    }
                    return Optional.<StatusWithError>empty();
                })
                .filter((k, sWithE) -> sWithE.isPresent())
                .mapValues((k, v) -> v.get());

        KStream<SagaId, SagaStateTransition> stateTransition = statusWithError.mapValues((sagaId, someStatus) ->
                SagaStateTransition.SagaStatusChanged.of(sagaId, someStatus.status, someStatus.errors.map(NonEmptyList::toList).orElse(Collections.emptyList())));

        KStream<SagaId, SagaResponse> sagaResponses = statusWithError
                .mapValues((sagaId, sWithE) -> {
                    SagaStatus status = sWithE.status;
                    if (status == SagaStatus.Completed)
                        return Optional.of(Result.<SagaError, Sequence>success(sWithE.sequence));
                    if (status == SagaStatus.Failed)
                        return Optional.of(Result.<SagaError, Sequence>failure(sWithE.errors.get()));
                    return Optional.<Result<SagaError, Sequence>>empty();
                })
                .filter((sagaId, v) -> v.isPresent())
                .mapValues((sagaId, v) -> SagaResponse.of(sagaId, v.get()));

        return Tuple2.of(stateTransition, sagaResponses);
    }

    static private <A> Tuple2<KStream<SagaId, SagaStateTransition>, KStream<SagaId, ActionRequest<A>>> addNextActions(
            KStream<SagaId, Saga<A>> sagaState) {

        // get the next actions from the state updates
        KStream<SagaId, List<SagaActionExecution<A>>> nextActionsListStream =
                sagaState.mapValues((k, state) -> SagaUtils.getNextActions(state));

        KStream<SagaId, SagaActionExecution<A>> nextActionsStream =
                nextActionsListStream.flatMapValues((k, v) -> v);

        KStream<SagaId, SagaStateTransition> stateUpdateNewActions = nextActionsListStream
                .filter((k, actions) -> !actions.isEmpty())
                .<SagaStateTransition>mapValues((sagaId, actions) -> {
                    List<SagaStateTransition.SagaActionStatusChanged> transitions = actions.stream().map(action ->
                            SagaStateTransition.SagaActionStatusChanged.of(sagaId, action.actionId, action.status, Collections.emptyList())
                    ).collect(Collectors.toList());
                    return SagaStateTransition.TransitionList.of(transitions);
                })
                .peek(logValues("stateUpdateNewActions"));

        KStream<SagaId, ActionRequest<A>> actionRequests =
                nextActionsStream
                        .filter((sagaId, v) -> v.command.isPresent())
                        .mapValues((sagaId, ae) ->
                                ActionRequest.<A>builder()
                                        .sagaId(sagaId)
                                        .actionId(ae.actionId)
                                        .actionCommand(ae.command.get())
                                        .actionType(ae.actionType)
                                        .build())
                        .peek(logValues("publishActionRequests"));

        return Tuple2.of(stateUpdateNewActions, actionRequests);
    }

    static private KStream<SagaId, SagaStateTransition> addActionResponses(KStream<SagaId, ActionResponse> actionResponses) {

        // TODO: simplify the error handling
        return actionResponses.<SagaStateTransition>mapValues((sagaId, response) -> {
            Tuple2<ActionStatus, List<SagaError>> se = response.result.fold(
                    errors -> Tuple2.of(ActionStatus.Failed, errors.toList()), // TODO: FIX this
                    r -> Tuple2.of(ActionStatus.Completed, Collections.emptyList()));
            return SagaStateTransition.SagaActionStatusChanged.of(sagaId, response.actionId, se.v1(), se.v2());
        }).peek(logValues("stateTransitionsActionResponse"));
    }
}
