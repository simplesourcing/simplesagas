package io.simplesource.saga.saga.internal;

import io.simplesource.data.NonEmptyList;
import io.simplesource.data.Result;
import io.simplesource.data.Sequence;
import io.simplesource.kafka.internal.util.Tuple2;
import io.simplesource.saga.model.action.ActionStatus;
import io.simplesource.saga.model.messages.SagaResponse;
import io.simplesource.saga.model.messages.SagaStateTransition;
import io.simplesource.saga.model.saga.Saga;
import io.simplesource.saga.model.saga.SagaError;
import io.simplesource.saga.model.saga.SagaId;
import io.simplesource.saga.model.saga.SagaStatus;
import lombok.Value;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

final class ResponseStream {
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

    static <A> Tuple2<KStream<SagaId, SagaStateTransition<A>>, KStream<SagaId, SagaResponse>> addSagaResponse(KStream<SagaId, Saga<A>> sagaState) {
        KStream<SagaId, StatusWithError> statusWithError = sagaState
                .mapValues((k, saga) -> getStatusWithError(saga))
                .filter((k, sWithE) -> sWithE.isPresent())
                .mapValues((k, v) -> v.get());

        KStream<SagaId, SagaStateTransition<A>> stateTransition = statusWithError.mapValues((sagaId, someStatus) ->
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

    private static <A> Optional<StatusWithError> getStatusWithError(Saga<A> saga) {
        ActionStatuses states = ActionStatuses.of(saga);

        if (saga.status == SagaStatus.InProgress && states.completed())
            return StatusWithError.of(saga.sequence, SagaStatus.Completed);
        if (saga.status == SagaStatus.InProgress && states.failurePending())
            return StatusWithError.of(saga.sequence, SagaStatus.FailurePending);
        if ((saga.status == SagaStatus.InProgress || saga.status == SagaStatus.FailurePending) &&
                states.inFailure())
            return StatusWithError.of(saga.sequence, SagaStatus.InFailure);
        if ((saga.status == SagaStatus.InFailure || saga.status == SagaStatus.InProgress) &&
                states.failed()) {
            List<SagaError> errors = saga.actions.values().stream()
                    .filter(action -> action.status == ActionStatus.Failed)
                    .flatMap(action -> action.error.stream())
                    .collect(Collectors.toList());

            return StatusWithError.of(saga.sequence, errors);
        }
        return Optional.empty();
    }
}
