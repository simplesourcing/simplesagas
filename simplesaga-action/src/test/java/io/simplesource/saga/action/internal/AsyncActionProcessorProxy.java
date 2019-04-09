package io.simplesource.saga.action.internal;

import io.simplesource.saga.action.async.AsyncContext;
import io.simplesource.saga.model.serdes.TopicSerdes;
import io.simplesource.saga.model.messages.ActionRequest;
import io.simplesource.saga.model.messages.ActionResponse;
import io.simplesource.saga.model.saga.SagaId;
import io.simplesource.saga.shared.kafka.AsyncPublisher;

import java.util.function.Function;

// to avoid making AsyncInvoker public
public final class AsyncActionProcessorProxy {

    public static <A, D, K, O, R> void processRecord(
            AsyncContext<A, D, K, O, R> asyncContext,
            SagaId sagaId,
            ActionRequest<A> request,
            AsyncPublisher<SagaId, ActionResponse<A>> responsePublisher,
            Function<TopicSerdes<K, R>, AsyncPublisher<K, R>> outputPublisher) {
        AsyncInvoker.processActionRequest(asyncContext, sagaId, request, responsePublisher, outputPublisher);
    }
}
