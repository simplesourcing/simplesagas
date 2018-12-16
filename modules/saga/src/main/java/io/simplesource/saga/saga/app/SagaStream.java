package io.simplesource.saga.saga.app;

import io.simplesource.data.NonEmptyList;
import io.simplesource.data.Result;
import io.simplesource.data.Sequence;
import io.simplesource.kafka.internal.util.Tuple2;
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
    static Logger logger = LoggerFactory.getLogger(SagaStream.class);

    static <K, V> ForeachAction<K, V> logValues(String prefix) {
        return (k, v) -> logger.info("{}: {}={}", prefix, k.toString().substring(0, 6), v.toString());
    }

    public static <A> void addSubTopology(SagaContext<A> ctx,
                                          KStream<UUID, SagaRequest<A>> sagaRequestStream,
                                          KStream<UUID, SagaStateTransition> stateTransitionStream,
                                          KStream<UUID, Saga<A>> stateStream,
                                          KStream<UUID, ActionResponse> actionResponseStream) {

        // create the state table from the state stream
        KTable<UUID, Saga<A>> stateTable = createStateTable(ctx, stateStream);

        // add stream transformations
        KStream<UUID, SagaStateTransition> inputStateTransitions = addInitialState(ctx, sagaRequestStream, stateTable);
        Tuple2<KStream<UUID, SagaStateTransition>, KStream<UUID, ActionRequest<A>>> rtar = addNextActions(stateStream);
        KStream<UUID, SagaStateTransition> responseTransitions = addActionResponses(actionResponseStream);
        Tuple2<KStream<UUID, SagaStateTransition>, KStream<UUID, SagaResponse>> stsr = addSagaResponse(ctx, stateStream);
        KStream<UUID, Saga<A>> sagaState = applyStateTransitions(ctx, stateTransitionStream);

        // publish to all the output topics
        SagaProducer.actionRequests(ctx, rtar.v2());
        SagaProducer.sagaStateTransitions(ctx,
                inputStateTransitions,
                rtar.v1(),
                responseTransitions,
                stsr.v1());
        SagaProducer.sagaState(ctx, sagaState);
        SagaProducer.sagaResponses(ctx, stsr.v2());
    }

    static <A> KTable<UUID, Saga<A>> createStateTable(SagaContext<A> ctx, KStream<UUID, Saga<A>> stateStream) {
        return stateStream.groupByKey().reduce(
                (s1, s2) -> (s1.sequence.getSeq() > s2.sequence.getSeq()) ? s1 : s2,
                Materialized.with(ctx.sSerdes.uuid(), ctx.sSerdes.state()));
    }

    static <A> KStream<UUID, Saga<A>> applyStateTransitions(SagaContext<A> ctx,
                                                            KStream<UUID, SagaStateTransition> stateTransitionStream) {
        SagaSerdes<A> sSerdes = ctx.sSerdes;

        Materialized<UUID, Saga<A>, KeyValueStore<Bytes, byte[]>> materialized = Materialized
                .<UUID, Saga<A>, KeyValueStore<Bytes, byte[]>>as("saga_state_aggregation")
                .withKeySerde(sSerdes.uuid())
                .withValueSerde(sSerdes.state());

        // TODO: remove the random UUIDs
        return stateTransitionStream
                .groupByKey(Serialized.with(sSerdes.uuid(), sSerdes.transition()))
                .aggregate(() -> Saga.of(UUID.randomUUID(), new HashMap<>(), SagaStatus.NotStarted, Sequence.first()),
                        (k, t, s) -> SagaUtils.applyTransition(t, s),
                        materialized)
                .toStream();
    }

    static <A> KStream<UUID, SagaStateTransition> addInitialState(SagaContext<A> ctx,
                                                                     KStream<UUID, SagaRequest<A>> sagaRequestStream,
                                                                     KTable<UUID, Saga<A>> stateTable) {
        SagaSerdes<A> sSerdes = ctx.sSerdes;
        KStream<UUID, Tuple2<SagaRequest<A>, Boolean>> newRequestStream = sagaRequestStream.leftJoin(
                stateTable,
                (v1, v2) -> Tuple2.of(v1, v2 == null),
                Joined.with(sSerdes.uuid(), sSerdes.request(), sSerdes.state()))
                .filter((k, tuple) -> tuple.v2());

        return newRequestStream.mapValues((k, v) -> new SagaStateTransition.SetInitialState<>(v.v1().initialState));
    }

    @Value
    static class StatusWithError {
        Sequence sequence;
        SagaStatus status;
        Optional<NonEmptyList<SagaError>> errors;

        static Optional<StatusWithError> of(Sequence sequence, SagaStatus status) {
            return Optional.of(new StatusWithError(sequence, status, Optional.empty()));
        }

        static Optional<StatusWithError> of(Sequence sequence, SagaStatus status, List<SagaError> error) {
            return Optional.of(new StatusWithError(sequence, status, NonEmptyList.fromList(error)));
        }
    }

    static <A> Tuple2<KStream<UUID, SagaStateTransition>, KStream<UUID, SagaResponse>> addSagaResponse(SagaContext<A> ctx,
                                                                                                          KStream<UUID, Saga<A>> sagaState) {
        KStream<UUID, StatusWithError> statusWithError = sagaState
                .mapValues((k, state) -> {
                    if (state.status == SagaStatus.InProgress && SagaUtils.sagaCompleted(state))
                        return StatusWithError.of(state.sequence, SagaStatus.Completed);
                    if (state.status == SagaStatus.InProgress && SagaUtils.sagaInFailure(state))
                        return StatusWithError.of(state.sequence, SagaStatus.InFailure);
                    if ((state.status == SagaStatus.InFailure || state.status == SagaStatus.InProgress) &&
                            SagaUtils.sagaFailed(state)) {
//                        Set<SagaError> errors = state.actions.values().stream()
//                                .filter(action -> action.status == ActionStatus.Failed)
//                                .map(action -> action.error).orElse(Stream.empty())
//                                .flatMap(x -> x).collect(Collectors.toSet());
                        List<SagaError> errors = state.actions.values().stream()
                                .filter(action -> action.status == ActionStatus.Failed && action.error.isPresent())
                                .map(action -> action.error.get())
                                .collect(Collectors.toList());
//
//                        NonEmptyList<String> errorList = NonEmptyList.fromList(new ArrayList<>(errors));
                        return StatusWithError.of(state.sequence, SagaStatus.Failed, errors);
                    }
                    return Optional.<StatusWithError>empty();
                })
                .filter((k, sWithE) -> sWithE.isPresent())
                .mapValues((k, v) -> v.get());

        KStream<UUID, SagaStateTransition> stateTransition = statusWithError.mapValues((sagaId, someStatus) ->
                new SagaStateTransition.SagaStatusChanged(sagaId, someStatus.status, someStatus.errors));

        KStream<UUID, SagaResponse> sagaResponses = statusWithError
                .mapValues((sagaId, sWithE) -> {
                    SagaStatus status = sWithE.status;
                    if (status == SagaStatus.Completed)
                        return Optional.of(Result.<SagaError, Sequence>success(sWithE.sequence));
                    if (status == SagaStatus.Failed)
                        return Optional.of(Result.<SagaError, Sequence>failure(sWithE.errors.get()));
                    return Optional.<Result<SagaError, Sequence>>empty();
                })
                .filter((sagaId, v) -> v.isPresent())
                .mapValues((sagaId, v) -> new SagaResponse(sagaId, v.get()));

        return Tuple2.of(stateTransition, sagaResponses);
    }

    static private <A> Tuple2<KStream<UUID, SagaStateTransition>, KStream<UUID, ActionRequest<A>>> addNextActions(
            KStream<UUID, Saga<A>> sagaState) {

        // get the next actions from the state updates
        KStream<UUID, List<SagaActionExecution<A>>> nextActionsListStream =
                sagaState.mapValues((k, state) -> SagaUtils.getNextActions(state));

        KStream<UUID, SagaActionExecution<A>> nextActionsStream =
                nextActionsListStream.flatMapValues((k, v) -> v);

        KStream<UUID, SagaStateTransition> stateUpdateNewActions = nextActionsListStream
                .filter((k, actions) -> !actions.isEmpty())
                .<SagaStateTransition>mapValues((sagaId, actions) -> {
                    List<SagaStateTransition.SagaActionStatusChanged> transitions = actions.stream().map(action ->
                            new SagaStateTransition.SagaActionStatusChanged(sagaId, action.actionId, action.status, Optional.empty())
                    ).collect(Collectors.toList());
                    return new SagaStateTransition.TransitionList(transitions);
                })
                .peek(logValues("stateUpdateNewActions"));

        KStream<UUID, ActionRequest<A>> actionRequests =
                nextActionsStream
                        .filter((sagaId, v) -> v.command.isPresent())
                        .mapValues((sagaId, ae) ->
                                new ActionRequest<>(sagaId,
                                        ae.actionId,
                                        ae.command.get(),
                                        ae.actionType))
                        .peek(logValues("actionRequests"));

        return Tuple2.of(stateUpdateNewActions, actionRequests);
    }

    static private KStream<UUID, SagaStateTransition> addActionResponses(KStream<UUID, ActionResponse> actionResponses) {

        // TODO: fix and simplify the error handling
        return actionResponses.<SagaStateTransition>mapValues((sagaId, response) -> {
            Tuple2<ActionStatus, Optional<SagaError>> se = response.result.fold(
                    errors -> Tuple2.of(ActionStatus.Failed, Optional.of(errors.head())), // TODO: FIX this
                    r -> Tuple2.of(ActionStatus.Completed, Optional.empty()));
            return new SagaStateTransition.SagaActionStatusChanged(sagaId, response.actionId, se.v1(), se.v2());
        }).peek(logValues("stateTransitionsActionResponse"));
    }
}
