package io.simplesource.saga.action.internal;

import io.simplesource.saga.action.async.AsyncContext;
import io.simplesource.saga.model.messages.ActionRequest;
import io.simplesource.saga.model.messages.ActionResponse;
import org.apache.kafka.streams.kstream.KStream;

import java.util.UUID;

final class AsyncStream {

    static <A, I, K, O, R> void addSubTopology(AsyncContext<A, I, K, O, R> ctx,
                                               KStream<UUID, ActionRequest<A>> actionRequest,
                                               KStream<UUID, ActionResponse> actionResponse) {
        // join the action request with corresponding prior command responses
        IdempotentStream.IdempotentAction<A> idempotentAction = IdempotentStream.getActionRequestsWithResponse(
                ctx.actionSpec, actionRequest, actionResponse, ctx.asyncSpec.actionType);

        // publish to output topics
        ActionPublisher.publishActionResponse(ctx.getActionContext(), idempotentAction.priorResponses);
        ActionPublisher.publishActionRequest(ctx.getActionContext(), idempotentAction.unprocessedRequests, true);
    }
}
