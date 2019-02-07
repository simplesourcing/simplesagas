package io.simplesource.saga.action.common;

import java.util.UUID;

import io.simplesource.kafka.internal.util.Tuple2;
import io.simplesource.saga.model.messages.ActionRequest;
import io.simplesource.saga.model.messages.ActionResponse;
import io.simplesource.saga.model.serdes.ActionSerdes;
import lombok.Value;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.common.utils.Bytes;

/**
 * For a given action type, returns a stream of prior responses (that don't need to be processed again)
 * and a stream of unprocessed requests for that action type.
 */
public class IdempotentStream {

    @Value
    public static class IdempotentAction<A> {
        public final KStream<UUID, ActionResponse> processedResponses;
        public final KStream<UUID, ActionRequest<A>> unprocessedRequests;
    }

    public static <A> IdempotentAction<A> getActionRequestsWithResponse(
            ActionSerdes<A> actionSerdes,
            KStream<UUID, ActionRequest<A>> requestKStream,
            KStream<UUID, ActionResponse> responseKStream,
            String actionType) {

        Materialized<UUID, ActionResponse, KeyValueStore<Bytes, byte[]>> materializer = Materialized
                .<UUID, ActionResponse, KeyValueStore<Bytes, byte[]>>as(
                        "last_action_by_command_id_" + actionType)
                .withKeySerde(actionSerdes.uuid())
                .withValueSerde(actionSerdes.response());

        KTable<UUID, ActionResponse> actionByCommandId =
                responseKStream
                        .selectKey((k, aResp) -> aResp.commandId)
                        .groupByKey(Serialized.with(actionSerdes.uuid(), actionSerdes.response()))
                        .reduce((cr1, cr2) -> cr2, materializer);

        // stream with response joined with the corresponding request
        KStream<UUID, Tuple2<ActionRequest<A>, ActionResponse>> actionRequestWithResponse = requestKStream
                .filter((k, aReq) -> aReq.actionType.equals(actionType))
                .leftJoin(actionByCommandId,
                        Tuple2::of,
                        Joined.with(actionSerdes.uuid(), actionSerdes.request(), actionSerdes.response()));

        // split between unprocessed and prior-processed actions
        KStream<UUID, Tuple2<ActionRequest<A>, ActionResponse>>[] branches = actionRequestWithResponse
                .branch((k, v) -> v.v2() != null, (k, v) -> v.v2() == null);

        KStream<UUID, ActionResponse> processedResponses = branches[0].mapValues((k, v) -> v.v2());
        KStream<UUID, ActionRequest<A>> unprocessedRequests = branches[1].mapValues((k, v) -> v.v1());
        return new IdempotentAction<>(processedResponses, unprocessedRequests);
    }
}
