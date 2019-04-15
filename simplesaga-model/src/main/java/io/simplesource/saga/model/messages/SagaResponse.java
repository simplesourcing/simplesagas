package io.simplesource.saga.model.messages;

import io.simplesource.data.Result;
import io.simplesource.data.Sequence;
import io.simplesource.saga.model.saga.SagaError;
import io.simplesource.saga.model.saga.SagaId;
import lombok.Value;

import java.time.Duration;

/**
 * A response from saga execution, published by the saga coordinator in the saga response topic after saga completes (successfully, or with error).
 *
 * If the {@link io.simplesource.saga.model.api.SagaAPI SagaAPI} was used to generate the initial {@link  SagaRequest}, the response is then used to
 * complete the {@link io.simplesource.saga.model.api.SagaAPI#getSagaResponse(SagaId, Duration) SagaAPI.getSagaResponse} method asynchronously.
 */
@Value(staticConstructor = "of")
public class SagaResponse {
    /**
     * The saga id uniquely identifies the saga, and is used as the key for the saga request and responses.
     */
    public final SagaId sagaId;
    /**
     * Either and error if the saga failed to execute successfully, or the sequence number, which indicates the number of steps executed in the completed saga.
     */
    public final Result<SagaError, Sequence> result;
}
