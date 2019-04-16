package io.simplesource.saga.saga.internal;

import io.simplesource.data.NonEmptyList;
import io.simplesource.data.Result;
import io.simplesource.kafka.internal.util.Tuple2;
import io.simplesource.saga.model.messages.SagaRequest;
import io.simplesource.saga.model.messages.SagaResponse;
import io.simplesource.saga.model.messages.SagaStateTransition;
import io.simplesource.saga.model.saga.Saga;
import io.simplesource.saga.model.saga.SagaError;
import io.simplesource.saga.model.saga.SagaId;
import io.simplesource.saga.model.serdes.SagaSerdes;
import io.simplesource.saga.saga.app.SagaContext;
import org.apache.kafka.streams.kstream.Joined;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;

import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

final class SetupStream {

    static <A> Tuple2<KStream<SagaId, SagaRequest<A>>, KStream<SagaId, SagaResponse>> validateSagaRequests(
            SagaContext<A> ctx,
            KStream<SagaId, SagaRequest<A>> sagaRequestStream) {

        Set<String> actionTypes = ctx.actionTopicNamers.keySet();

        // check that all action types are valid (may add other validations at some point)
        KStream<SagaId, Tuple2<SagaRequest<A>, List<SagaError>>> y = sagaRequestStream.mapValues((id, request) -> {
            List<SagaError> errors = request.initialState.actions.values().stream().map(action -> {
                String at = action.command.actionType.toLowerCase();
                return !actionTypes.contains(at) ? SagaError.of(SagaError.Reason.InvalidSaga, String.format("Unknown action type '%s'", at)) : null;
            }).filter(Objects::nonNull).collect(Collectors.toList());
            return Tuple2.of(request, errors);
        });

        KStream<SagaId, Tuple2<SagaRequest<A>, List<SagaError>>>[] z = y.branch((k, reqWithErrors) -> reqWithErrors.v2().isEmpty(), (k, reqWithErrors) -> !reqWithErrors.v2().isEmpty());
        KStream<SagaId, SagaRequest<A>> validRequests = z[0].mapValues(Tuple2::v1);
        KStream<SagaId, SagaResponse> inValidResponses = z[1].mapValues(Tuple2::v2).mapValues((k, v) -> SagaResponse.of(k, Result.failure(NonEmptyList.fromList(v).get())));

        return Tuple2.of(validRequests, inValidResponses);
    }

    static <A> KStream<SagaId, SagaStateTransition<A>> addInitialState(SagaContext<A> ctx,
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
}
