package io.simplesource.saga.action.internal;

import io.simplesource.saga.action.async.AsyncContext;
import io.simplesource.saga.model.serdes.TopicSerdes;
import io.simplesource.saga.model.messages.ActionRequest;
import io.simplesource.saga.model.messages.ActionResponse;
import io.simplesource.saga.model.saga.SagaId;

import java.util.function.Function;

// to avoid making AsyncActionProcessor public
public final class AsyncActionProcessorProxy {

    public static <A, D, K, O, R> void processRecord(
            AsyncContext<A, D, K, O, R> asyncContext,
            SagaId sagaId,
            ActionRequest<A> request,
            AsyncPublisher<SagaId, ActionResponse> responsePublisher,
            Function<TopicSerdes<K, R>, AsyncPublisher<K, R>> outputPublisher) {
        AsyncActionProcessor.processRecord(asyncContext, sagaId, request, responsePublisher, outputPublisher);
    }
}
