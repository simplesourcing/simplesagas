package io.simplesource.saga.model.api;

import io.simplesource.data.FutureResult;
import io.simplesource.saga.model.messages.SagaRequest;
import io.simplesource.saga.model.messages.SagaResponse;
import io.simplesource.saga.model.saga.SagaError;
import io.simplesource.saga.model.saga.SagaId;

import java.time.Duration;

public interface SagaAPI<A> {
    FutureResult<SagaError, SagaId> submitSaga(SagaRequest<A> request);
    FutureResult<SagaError, SagaResponse> getSagaResponse(SagaId requestId, Duration timeout);
}
