package io.simplesource.saga.model.api;

import io.simplesource.data.FutureResult;
import io.simplesource.saga.model.messages.SagaRequest;
import io.simplesource.saga.model.messages.SagaResponse;
import io.simplesource.saga.model.saga.SagaError;

import java.time.Duration;
import java.util.UUID;

public interface SagaAPI<A> {
    FutureResult<SagaError, UUID> submitSaga(SagaRequest<A> request);
    FutureResult<SagaError, SagaResponse> getSagaResponse(UUID requestId, Duration timeout);
}
