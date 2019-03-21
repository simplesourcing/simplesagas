package io.simplesource.saga.action.internal;

import io.simplesource.api.CommandError;
import io.simplesource.data.NonEmptyList;
import io.simplesource.data.Result;
import io.simplesource.data.Sequence;
import io.simplesource.kafka.api.CommandSerdes;
import io.simplesource.kafka.internal.util.Tuple2;
import io.simplesource.kafka.model.CommandRequest;
import io.simplesource.kafka.model.CommandResponse;
import io.simplesource.saga.action.sourcing.SourcingContext;
import io.simplesource.saga.model.messages.ActionRequest;
import io.simplesource.saga.model.messages.ActionResponse;
import io.simplesource.saga.model.saga.SagaError;
import io.simplesource.saga.model.serdes.ActionSerdes;
import io.simplesource.saga.shared.serialization.TupleSerdes;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public final class SourcingStream {

    private static Logger logger = LoggerFactory.getLogger(SourcingStream.class);

    /**
     * Add a sub-topology for a simple sourcing command topic, including all the saga actions that map to that topic.
     *
     * @param topologyContext topology context.
     * @param sourcing        sourcing context.
     */
    public static <A, I, K, C> void addSubTopology(ActionTopologyBuilder.ActionTopologyContext<A> topologyContext,
                                                   SourcingContext<A, I, K, C> sourcing) {
        KStream<K, CommandResponse<K>> commandResponseStream = CommandConsumer.commandResponseStream(
                sourcing.commandSpec(), sourcing.commandTopicNamer(), topologyContext.builder());
        addSubTopology(sourcing, topologyContext.actionRequests(), topologyContext.actionResponses(), commandResponseStream);
    }

    private static <A, I, K, C> void addSubTopology(SourcingContext<A, I, K, C> ctx,
                                                    KStream<UUID, ActionRequest<A>> actionRequest,
                                                    KStream<UUID, ActionResponse> actionResponse,
                                                    KStream<K, CommandResponse<K>> commandResponseByAggregate) {
        KStream<UUID, CommandResponse<K>> commandResponseByCommandId = commandResponseByAggregate.selectKey((k, v) -> v.commandId());

        IdempotentStream.IdempotentAction<A> idempotentAction = IdempotentStream.getActionRequestsWithResponse(ctx.actionSpec,
                actionRequest,
                actionResponse,
                ctx.commandSpec.actionType);
        // get new command requests
        Tuple2<KStream<UUID, ActionResponse>, KStream<K, CommandRequest<K, C>>> requestResp = handleActionRequest(ctx, idempotentAction.unprocessedRequests, commandResponseByAggregate);
        KStream<UUID, ActionResponse> requestErrorResponses = requestResp.v1();
        KStream<K, CommandRequest<K, C>> commandRequests = requestResp.v2();

        // handle incoming command responses
        KStream<UUID, ActionResponse> newActionResponses = handleCommandResponse(ctx, actionRequest, commandResponseByCommandId);

        ActionContext<A> actionCtx = ctx.getActionContext();

        CommandPublisher.publishCommandRequest(ctx, commandRequests);
        ActionPublisher.publishActionResponse(actionCtx, idempotentAction.priorResponses());
        ActionPublisher.publishActionResponse(actionCtx, newActionResponses);
        ActionPublisher.publishActionResponse(actionCtx, requestErrorResponses);
    }


    /**
     * Unfortunately we have to keep involing this decoder step
     */
    private static <A, I> I getIntermediate(SourcingContext<A, I, ?, ?> ctx, ActionRequest<A> aReq) {
        I i = ctx.commandSpec.decode.apply(aReq.actionCommand.command).getOrElse(null);
        assert i != null; // this should have already been checked
        return i;
    }

    /**
     * Translate simplesaga action requests to simplesourcing command requests.
     */
    private static <A, I, K, C> Tuple2<KStream<UUID, ActionResponse>, KStream<K, CommandRequest<K, C>>> handleActionRequest(
            SourcingContext<A, I, K, C> ctx,
            KStream<UUID, ActionRequest<A>> actionRequests,
            KStream<K, CommandResponse<K>> commandResponseByAggregate) {

        KStream<UUID, Tuple2<ActionRequest<A>, Result<Throwable, I>>> reqsWithDecoded =
                actionRequests
                        .mapValues((k, ar) -> Tuple2.of(ar, ctx.commandSpec.decode.apply(ar.actionCommand.command)))
                        .peek(Utils.logValues(logger, "reqsWithDecoded"));

        KStream<UUID, Tuple2<ActionRequest<A>, Result<Throwable, I>>>[] branchSuccessFailure = reqsWithDecoded.branch((k, v) -> v.v2().isSuccess(), (k, v) -> v.v2().isFailure());

        KStream<UUID, ActionResponse> errorActionResponses = branchSuccessFailure[1].mapValues((k, v) -> {
            ActionRequest<A> request = v.v1();
            NonEmptyList<Throwable> reasons = v.v2().failureReasons().get();
            return new ActionResponse(request.sagaId, request.actionId, request.actionCommand.commandId, Result.failure(
                    SagaError.of(SagaError.Reason.InternalError, reasons.head())));
        });

        KStream<UUID, Tuple2<ActionRequest<A>, I>> allGood = reqsWithDecoded
                .mapValues((k, v) -> Tuple2.of(v.v1(), v.v2().getOrElse(null)));

        KTable<Tuple2<K, UUID>, Long> latestSequenceNumbers = latestSequenceNumbersForSagaAggregate(
                ctx, actionRequests, commandResponseByAggregate);


        ValueJoiner<ActionRequest<A>, Long, CommandRequest<K, C>> valueJoiner =
                (aReq, seq) -> {
                    // we can do this safely as we have (unfortunately) already done this, and succeeded first time
                    I intermediate = getIntermediate(ctx, aReq);

                    // use the input sequence for the first action, and the last sequence number for the aggregate in the saga otherwise
                    Sequence sequence = (seq == null) ?
                            ctx.commandSpec.sequenceMapper.apply(intermediate) :
                            Sequence.position(seq);
                    return new CommandRequest<>(ctx.commandSpec.keyMapper.apply(intermediate),
                            ctx.commandSpec.commandMapper.apply(intermediate),
                            sequence,
                            aReq.actionCommand.commandId);
                };

        KStream<K, CommandRequest<K, C>> commandRequestByAggregate = allGood.map((k, v) ->
        {
            I intermediate = getIntermediate(ctx, v.v1());
            K key = ctx.commandSpec.keyMapper.apply(intermediate);
            return KeyValue.pair(Tuple2.of(key, v.v1().sagaId), v.v1());
        })
                .leftJoin(latestSequenceNumbers, valueJoiner,
                        Joined.with(TupleSerdes.tuple2(ctx.cSerdes().aggregateKey(), ctx.aSerdes().uuid()), ctx.aSerdes().request(), Serdes.Long()))
                .selectKey((k, v) -> v.aggregateKey())
                .peek(Utils.logValues(logger, "commandRequestByAggregate"));

        return Tuple2.of(errorActionResponses, commandRequestByAggregate);
    }

    private static <A, K> KTable<Tuple2<K, UUID>, Long> latestSequenceNumbersForSagaAggregate(
            SourcingContext<A, ?, K, ?> ctx,
            KStream<UUID, ActionRequest<A>> actionRequests,
            KStream<K, CommandResponse<K>> commandResponseByAggregate) {
        CommandSerdes<K, ?> cSerdes = ctx.cSerdes();
        ActionSerdes<A> aSerdes = ctx.aSerdes();

        Serde<Tuple2<K, UUID>> sagaAggKeySerde = TupleSerdes.tuple2(cSerdes.aggregateKey(), aSerdes.uuid());
        Reducer<Long> reducer = (cr1, cr2) -> cr2 > cr1 ? cr2 : cr1;

        // Join the action request and command response by commandID
        KStream<UUID, Tuple2<CommandResponse<K>, ActionRequest<A>>> crArByCi = commandResponseByAggregate
                .selectKey((k, v) -> v.commandId())
                .join(actionRequests.selectKey((k, v) -> v.actionCommand.commandId), Tuple2::of, JoinWindows.of(0L),
                        Joined.with(cSerdes.commandResponseKey(), cSerdes.commandResponse(), aSerdes.request()))
                .peek(Utils.logValues(logger, "crArByCi"));


        // Get the stream of sequence numbers keyed by (aggregate key, sagaID)
        KStream<Tuple2<K, UUID>, Long> snByAkSi = crArByCi
                .selectKey((k, v) -> Tuple2.of(v.v1().aggregateKey(), v.v2().sagaId()))
                .mapValues(v -> v.v1().sequenceResult().getOrElse(Sequence.first()).getSeq())
                .peek(Utils.logValues(logger, "snByAkSi"));

        // Get the table of the largest sequence number keyed by (aggregate key, sagaID)
        return snByAkSi
                .groupByKey(Serialized.with(sagaAggKeySerde, Serdes.Long()))
                .reduce(reducer, Materialized.with(sagaAggKeySerde, Serdes.Long()));
}

    /**
     * Receives command response from simplesourcing, and convert to simplesaga action response.
     */
    private static <A, I, K, C> KStream<UUID, ActionResponse> handleCommandResponse(
            SourcingContext<A, I, K, C> ctx,
            KStream<UUID, ActionRequest<A>> actionRequests,
            KStream<UUID, CommandResponse<K>> responseByCommandId) {
        long timeOutMillis = ctx.commandSpec.timeOutMillis;
        // find the response for the request
        KStream<UUID, Tuple2<ActionRequest<A>, CommandResponse<K>>> actionRequestWithResponse =

                // join command response to action request by the command / action ID
                // TODO: timeouts - will be easy to do timeouts with a left join once https://issues.apache.org/jira/browse/KAFKA-6556 has been released
                actionRequests
                        .selectKey((k, aReq) -> aReq.actionCommand.commandId)
                        .join(
                                responseByCommandId,
                                Tuple2::of,
                                JoinWindows.of(timeOutMillis).until(timeOutMillis * 2 + 1),
                                Joined.with(ctx.aSerdes().uuid(), ctx.aSerdes().request(), ctx.cSerdes().commandResponse())
                        )
                        .peek(Utils.logValues(logger, "joinActionRequestAndCommandResponse"));

        // turn the pair into an ActionResponse
        return actionRequestWithResponse
                .mapValues((k, v) -> {
                            ActionRequest<A> aReq = v.v1();
                            CommandResponse<K> cResp = v.v2();
                            Result<CommandError, Sequence> sequenceResult =
                                    (cResp == null) ?
                                            Result.failure(CommandError.of(CommandError.Reason.Timeout,
                                                    "Timed out waiting for response from Command Processor")) :
                                            cResp.sequenceResult();
                            Result<SagaError, Boolean> result =
                                    sequenceResult.fold(errors -> {
                                                String message = String.join(",", errors.map(CommandError::getMessage));
                                                return Result.failure(SagaError.of(SagaError.Reason.CommandError, message));
                                            },
                                            seq -> Result.success(true));

                            return new ActionResponse(aReq.sagaId,
                                    aReq.actionId,
                                    aReq.actionCommand.commandId,
                                    result);
                        }
                )
                .selectKey((k, resp) -> resp.sagaId)
                .peek(Utils.logValues(logger, "resultStream"));
    }

}
