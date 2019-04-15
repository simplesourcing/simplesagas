package io.simplesource.saga.model.api;

import io.simplesource.data.FutureResult;
import io.simplesource.saga.model.messages.SagaRequest;
import io.simplesource.saga.model.messages.SagaResponse;
import io.simplesource.saga.model.saga.SagaError;
import io.simplesource.saga.model.saga.SagaId;

import java.time.Duration;

/**
 * The Saga API, used for submitting saga requests and asynchronously waiting for the result of the saga execution/
 *
 * @param <A> A representation of an action command that is shared across all actions in the saga. This is typically a generic type, such as Json, or if using Avro serialization, SpecificRecord or GenericRecord
 */
public interface SagaAPI<A> {
    /**
     * Initiates a saga.
     * <p>
     * This does not return the result of the saga, just an indication of whether the request was published successfully or not.
     * We call the {@link SagaAPI#getSagaResponse(SagaId, Duration) getSagaResponse} method to asychronously return the result of the saga.
     *
     * @param request the request
     * @return a future result with the saga Id, or an error if the saga request was not successfully published
     */
    FutureResult<SagaError, SagaId> submitSaga(SagaRequest<A> request);

    /**
     * Returns the result of the saga.
     *
     * @param sagaId the saga id for the saga initiated by calling {@link SagaAPI#submitSaga(SagaRequest) submitSaga}.
     * @param timeout   the timeout
     * @return a future result with the saga response, or an error if the saga was not successfully processed
     */
    FutureResult<SagaError, SagaResponse> getSagaResponse(SagaId sagaId, Duration timeout);
}
