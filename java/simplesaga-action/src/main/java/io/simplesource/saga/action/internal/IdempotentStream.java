package io.simplesource.saga.action.internal;

import java.util.UUID;

import io.simplesource.api.CommandId;
import io.simplesource.kafka.internal.util.Tuple2;
import io.simplesource.saga.model.messages.ActionRequest;
import io.simplesource.saga.model.messages.ActionResponse;
import io.simplesource.saga.model.specs.ActionProcessorSpec;
import lombok.Value;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.common.utils.Bytes;

class IdempotentStream {

    @Value
    public static class IdempotentAction<A> {
        public final KStream<UUID, ActionResponse> priorResponses;
        public final KStream<UUID, ActionRequest<A>> unprocessedRequests;
    }

    static <A> IdempotentAction<A> getActionRequestsWithResponse
            (ActionProcessorSpec<A> aSpec,
             KStream<UUID, ActionRequest<A>> actionRequests,
             KStream<UUID, ActionResponse> actionResponse,
             String actionType) {

        KTable<CommandId, ActionResponse> actionByCommandId =
                actionResponse
                        .selectKey((k, aResp) -> aResp.commandId)
                        .groupByKey(Serialized.with(aSpec.serdes.commandId(), aSpec.serdes.response()))
                        .reduce((cr1, cr2) -> cr2, Materialized.with(aSpec.serdes.commandId(), aSpec.serdes.response()));

        KStream<UUID, Tuple2<ActionRequest<A>, ActionResponse>> actionRequestWithResponse = actionRequests
                .filter((k, aReq) -> aReq.actionType.equals(actionType))
                .selectKey((k, v) -> v.actionCommand.commandId)
                .leftJoin(actionByCommandId,
                        (k, v) -> Tuple2.of(k, v),
                        Joined.with(aSpec.serdes.commandId(), aSpec.serdes.request(), aSpec.serdes.response()))
                .selectKey(((k, v) -> v.v1().sagaId));

        // split between unprocessed and prior-processed actions
        KStream<UUID, Tuple2<ActionRequest<A>, ActionResponse>>[] branches = actionRequestWithResponse
                .branch((k, v) -> v.v2() != null, (k, v) -> v.v2() == null);

        // TODO: remove the processedResponses stream, as the handleCommandResponse already returns an action response if a command response has already been published - suppressed for now
        KStream<UUID, ActionResponse> processedResponses = branches[0].mapValues((k, v) -> v.v2()).filter((k, v) -> false);
        KStream<UUID, ActionRequest<A>> unprocessedRequests = branches[1].mapValues((k, v) -> v.v1());
        return new IdempotentAction<>(processedResponses, unprocessedRequests);
    }
}
