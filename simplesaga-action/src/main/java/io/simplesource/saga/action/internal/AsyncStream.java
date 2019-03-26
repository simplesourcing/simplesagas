package io.simplesource.saga.action.internal;

import io.simplesource.saga.action.async.AsyncContext;
import io.simplesource.saga.model.messages.ActionRequest;
import io.simplesource.saga.model.messages.ActionResponse;
import io.simplesource.saga.model.saga.SagaId;
import org.apache.kafka.streams.kstream.KStream;

public final class AsyncStream {

    public static <A, D, K, O, R> AsyncPipe addSubTopology(ActionTopologyContext<A> topologyContext,
                                                           AsyncContext<A, D, K, O, R> async) {
        addSubTopology(async, topologyContext.actionRequests, topologyContext.actionResponses);
        // create a Kafka consumer that processes action requests
        return AsyncTransform.async(async, topologyContext.properties);
    }

    private static <A, D, K, O, R> void addSubTopology(AsyncContext<A, D, K, O, R> ctx,
                                                       KStream<SagaId, ActionRequest<A>> actionRequest,
                                                       KStream<SagaId, ActionResponse> actionResponse) {
        // join the action request with corresponding prior command responses
        IdempotentStream.IdempotentAction<A> idempotentAction = IdempotentStream.getActionRequestsWithResponse(
                ctx.actionSpec, actionRequest, actionResponse, ctx.asyncSpec.actionType);

        // publish to output topics
        ActionPublisher.publishActionResponse(ctx.getActionContext(), idempotentAction.priorResponses);
        ActionPublisher.publishActionRequest(ctx.getActionContext(), idempotentAction.unprocessedRequests, true);
    }
}
