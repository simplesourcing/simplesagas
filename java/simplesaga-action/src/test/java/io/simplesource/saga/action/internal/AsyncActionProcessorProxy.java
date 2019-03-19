package io.simplesource.saga.action.internal;

import io.simplesource.saga.action.async.AsyncContext;
import io.simplesource.saga.action.async.AsyncSerdes;
import io.simplesource.saga.model.messages.ActionRequest;
import io.simplesource.saga.model.messages.ActionResponse;

import java.util.UUID;
import java.util.function.Function;

// to avoid making AsyncActionProcessor public
public final class AsyncActionProcessorProxy {

    public static <A, I, K, O, R> void processRecord(
            AsyncContext<A, I, K, O, R> asyncContext,
            UUID sagaId, ActionRequest<A> request,
            AsyncPublisher<UUID, ActionResponse> responsePublisher,
            Function<AsyncSerdes<K, R>, AsyncPublisher<K, R>> outputPublisher) {
        AsyncActionProcessor.processRecord(asyncContext, sagaId, request, responsePublisher, outputPublisher);
    }
}
