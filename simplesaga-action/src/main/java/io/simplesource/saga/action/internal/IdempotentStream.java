package io.simplesource.saga.action.internal;

import io.simplesource.api.CommandId;
import io.simplesource.kafka.internal.util.Tuple2;
import io.simplesource.saga.model.messages.ActionRequest;
import io.simplesource.saga.model.messages.ActionResponse;
import io.simplesource.saga.model.saga.SagaId;
import io.simplesource.saga.model.specs.ActionSpec;
import lombok.Value;
import org.apache.kafka.streams.kstream.*;

final class IdempotentStream {

    @Value
    public static class IdempotentAction<A> {
        public final KStream<SagaId, ActionResponse> priorResponses;
        public final KStream<SagaId, ActionRequest<A>> unprocessedRequests;
    }

    static <A> IdempotentAction<A> getActionRequestsWithResponse
            (ActionSpec<A> aSpec,
             KStream<SagaId, ActionRequest<A>> actionRequests,
             KStream<SagaId, ActionResponse> actionResponse) {

        KTable<CommandId, ActionResponse> actionByCommandId =
                actionResponse
                        .selectKey((k, aResp) -> aResp.commandId)
                        .groupByKey(Grouped.with(aSpec.serdes.commandId(), aSpec.serdes.response()))
                        .reduce((cr1, cr2) -> cr2, Materialized.with(aSpec.serdes.commandId(), aSpec.serdes.response()));

        KStream<SagaId, Tuple2<ActionRequest<A>, ActionResponse>> actionRequestWithResponse = actionRequests
                .selectKey((k, v) -> v.actionCommand.commandId)
                .leftJoin(actionByCommandId,
                        Tuple2::of,
                        Joined.with(aSpec.serdes.commandId(), aSpec.serdes.request(), aSpec.serdes.response()))
                .selectKey(((k, v) -> v.v1().sagaId));

        // split between unprocessed and prior-processed actions
        KStream<SagaId, Tuple2<ActionRequest<A>, ActionResponse>>[] branches = actionRequestWithResponse
                .branch((k, v) -> v.v2() != null, (k, v) -> v.v2() == null);

        // TODO: remove the processedResponses stream, as the handleCommandResponse already returns an action response if a command response has already been published - suppressed for now
        KStream<SagaId, ActionResponse> processedResponses = branches[0].mapValues((k, v) -> v.v2()).filter((k, v) -> false);
        KStream<SagaId, ActionRequest<A>> unprocessedRequests = branches[1].mapValues((k, v) -> v.v1());
        return new IdempotentAction<>(processedResponses, unprocessedRequests);
    }
}
