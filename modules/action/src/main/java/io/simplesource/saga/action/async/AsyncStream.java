package io.simplesource.saga.action.async;

import java.util.UUID;

import io.simplesource.saga.action.common.ActionProducer;
import io.simplesource.saga.action.common.IdempotentStream;
import io.simplesource.saga.action.common.Utils;
import io.simplesource.saga.model.messages.ActionRequest;
import io.simplesource.saga.model.messages.ActionResponse;
import org.apache.kafka.streams.kstream.ForeachAction;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final public class AsyncStream {
  static Logger logger = LoggerFactory.getLogger(AsyncStream.class);

  static <K, V> ForeachAction<K, V> logValues(String prefix) {
    return Utils.logValues(logger, prefix);
  }

  static public <A, I, K, O, R> void addSubTopology(AsyncContext<A, I, K, O, R> ctx,
                                    KStream<UUID, ActionRequest<A>> actionRequest,
                                    KStream<UUID, ActionResponse> actionResponse) {
    // join the action request with corresponding prior io.simplesource.io.simplesource.saga.user.saga.user.command responses
    IdempotentStream.IdempotentAction<A> idempotentAction = IdempotentStream.getActionRequestsWithResponse(ctx.actionSpec,
            actionRequest,
            actionResponse,
            ctx.asyncSpec.actionType);

    // publish to output topics
    ActionProducer.actionResponse(ctx.actionSpec, ctx.actionTopicNamer, idempotentAction.priorResponses);
    ActionProducer.actionRequest(ctx.actionSpec,
                                 ctx.actionTopicNamer,
                                 idempotentAction.unprocessedRequests,
                                 true);
  }
}
