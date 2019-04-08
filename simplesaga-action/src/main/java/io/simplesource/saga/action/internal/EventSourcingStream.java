package io.simplesource.saga.action.internal;

import io.simplesource.api.CommandError;
import io.simplesource.api.CommandId;
import io.simplesource.data.NonEmptyList;
import io.simplesource.data.Result;
import io.simplesource.data.Sequence;
import io.simplesource.kafka.api.CommandSerdes;
import io.simplesource.kafka.internal.util.Tuple2;
import io.simplesource.kafka.model.CommandRequest;
import io.simplesource.kafka.model.CommandResponse;
import io.simplesource.saga.action.eventsourcing.EventSourcingContext;
import io.simplesource.saga.model.messages.ActionRequest;
import io.simplesource.saga.model.messages.ActionResponse;
import io.simplesource.saga.model.messages.UndoCommand;
import io.simplesource.saga.model.saga.SagaError;
import io.simplesource.saga.model.saga.SagaId;
import io.simplesource.saga.model.serdes.ActionSerdes;
import io.simplesource.saga.shared.serialization.TupleSerdes;
import io.simplesource.saga.shared.streams.StreamUtils;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Optional;
import java.util.function.BiFunction;

public final class EventSourcingStream {

    private static Logger logger = LoggerFactory.getLogger(EventSourcingStream.class);

    /**
     * Add a sub-topology for a simple sourcing command topic, including all the saga actions that map to that topic.
     *
     * @param topologyContext topology context.
     * @param sourcing        sourcing context.
     */
    public static <A, D, K, C> void addSubTopology(ActionTopologyContext<A> topologyContext,
                                                   EventSourcingContext<A, D, K, C> sourcing) {
        KStream<K, CommandResponse<K>> commandResponseStream = EventSourcingConsumer.commandResponseStream(
                sourcing.eventSourcingSpec, sourcing.commandTopicNamer, topologyContext.builder);
        addSubTopology(sourcing, topologyContext.actionRequests, topologyContext.actionResponses, commandResponseStream);
    }

    private static <A, D, K, C> void addSubTopology(EventSourcingContext<A, D, K, C> ctx,
                                                    KStream<SagaId, ActionRequest<A>> actionRequest,
                                                    KStream<SagaId, ActionResponse<A>> actionResponse,
                                                    KStream<K, CommandResponse<K>> commandResponseByAggregate) {
        KStream<CommandId, CommandResponse<K>> commandResponseByCommandId = commandResponseByAggregate.selectKey((k, v) -> v.commandId());

        IdempotentStream.IdempotentAction<A> idempotentAction = IdempotentStream.getActionRequestsWithResponse(ctx.actionSpec,
                actionRequest,
                actionResponse);

        // get new command requests
        Tuple2<KStream<SagaId, ActionResponse<A>>, KStream<K, CommandRequest<K, C>>> requestResp =
                handleActionRequest(ctx, idempotentAction.unprocessedRequests, commandResponseByAggregate);

        KStream<SagaId, ActionResponse<A>> requestErrorResponses = requestResp.v1();
        KStream<K, CommandRequest<K, C>> commandRequests = requestResp.v2();

        // handle incoming command responses
        KStream<SagaId, ActionResponse<A>> newActionResponses = handleCommandResponse(ctx, actionRequest, commandResponseByCommandId);

        ActionContext<A> actionCtx = ctx.getActionContext();

        EventSourcingPublisher.publishCommandRequest(ctx, commandRequests);
        ActionPublisher.publishActionResponse(actionCtx, idempotentAction.priorResponses);
        ActionPublisher.publishActionResponse(actionCtx, newActionResponses);
        ActionPublisher.publishActionResponse(actionCtx, requestErrorResponses);
    }

    /**
     * Unfortunately we have to keep invoking this decoder step
     */
    private static <A, D> D getDecoded(EventSourcingContext<A, D, ?, ?> ctx, ActionRequest<A> aReq) {
        D d = ctx.eventSourcingSpec.decode.apply(aReq.actionCommand.command).getOrElse(null);
        assert d != null; // this should have already been checked
        return d;
    }

    /**
     * Translate simplesaga action requests to simplesourcing command requests.
     */
    private static <A, D, K, C> Tuple2<KStream<SagaId, ActionResponse<A>>, KStream<K, CommandRequest<K, C>>> handleActionRequest(
            EventSourcingContext<A, D, K, C> ctx,
            KStream<SagaId, ActionRequest<A>> actionRequests,
            KStream<K, CommandResponse<K>> commandResponseByAggregate) {

        KStream<SagaId, Tuple2<ActionRequest<A>, Result<Throwable, D>>> reqsWithDecoded =
                actionRequests
                        .mapValues((k, ar) -> Tuple2.of(ar, ctx.eventSourcingSpec.decode.apply(ar.actionCommand.command)))
                        .peek(StreamUtils.logValues(logger, "reqsWithDecoded"));

        KStream<SagaId, Tuple2<ActionRequest<A>, Result<Throwable, D>>>[] branchSuccessFailure = reqsWithDecoded.branch((k, v) -> v.v2().isSuccess(), (k, v) -> v.v2().isFailure());

        KStream<SagaId, ActionResponse<A>> errorActionResponses = branchSuccessFailure[1].mapValues((k, v) -> {
            ActionRequest<A> request = v.v1();
            NonEmptyList<Throwable> reasons = v.v2().failureReasons().get();
            return ActionResponse.of(request.sagaId,
                    request.actionId,
                    request.actionCommand.commandId,
                    request.isUndo,
                    Result.failure(
                    SagaError.of(SagaError.Reason.InternalError, reasons.head())));
        });

        KStream<SagaId, Tuple2<ActionRequest<A>, D>> allGood = reqsWithDecoded
                .mapValues((k, v) -> Tuple2.of(v.v1(), v.v2().getOrElse(null)));

        KTable<Tuple2<K, SagaId>, Long> latestSequenceNumbers = latestSequenceNumbersForSagaAggregate(
                ctx, actionRequests, commandResponseByAggregate);


        ValueJoiner<ActionRequest<A>, Long, CommandRequest<K, C>> valueJoiner =
                (aReq, seq) -> {
                    // we can do this safely as we have (unfortunately) already done this, and succeeded first time
                    D decoded = getDecoded(ctx, aReq);

                    // use the input sequence for the first action, and the last sequence number for the aggregate in the saga otherwise
                    Sequence sequence = (seq == null) ?
                            ctx.eventSourcingSpec.sequenceMapper.apply(decoded) :
                            Sequence.position(seq);
                    return new CommandRequest<>(
                            aReq.actionCommand.commandId,
                            ctx.eventSourcingSpec.keyMapper.apply(decoded),
                            sequence,
                            ctx.eventSourcingSpec.commandMapper.apply(decoded));
                };

        KStream<K, CommandRequest<K, C>> commandRequestByAggregate = allGood.map((k, v) ->
        {
            D decoded = getDecoded(ctx, v.v1());
            K key = ctx.eventSourcingSpec.keyMapper.apply(decoded);
            return KeyValue.pair(Tuple2.of(key, v.v1().sagaId), v.v1());
        })
                .leftJoin(latestSequenceNumbers, valueJoiner,
                        Joined.with(TupleSerdes.tuple2(ctx.cSerdes().aggregateKey(), ctx.aSerdes().sagaId()), ctx.aSerdes().request(), Serdes.Long()))
                .selectKey((k, v) -> v.aggregateKey())
                .peek(StreamUtils.logValues(logger, "commandRequestByAggregate"));

        return Tuple2.of(errorActionResponses, commandRequestByAggregate);
    }

    private static <A, K> KTable<Tuple2<K, SagaId>, Long> latestSequenceNumbersForSagaAggregate(
            EventSourcingContext<A, ?, K, ?> ctx,
            KStream<SagaId, ActionRequest<A>> actionRequests,
            KStream<K, CommandResponse<K>> commandResponseByAggregate) {
        CommandSerdes<K, ?> cSerdes = ctx.cSerdes();
        ActionSerdes<A> aSerdes = ctx.aSerdes();

        Serde<Tuple2<K, SagaId>> sagaAggKeySerde = TupleSerdes.tuple2(cSerdes.aggregateKey(), aSerdes.sagaId());
        Reducer<Long> reducer = (cr1, cr2) -> cr2 > cr1 ? cr2 : cr1;

        // Join the action request and command response by commandID
        KStream<CommandId, Tuple2<CommandResponse<K>, ActionRequest<A>>> crArByCi = commandResponseByAggregate
                .selectKey((k, v) -> v.commandId())
                .join(actionRequests.selectKey((k, v) -> v.actionCommand.commandId), Tuple2::of, JoinWindows.of(ctx.eventSourcingSpec.timeout).until(ctx.eventSourcingSpec.timeout.toMillis() * 2 + 1),
                        Joined.with(cSerdes.commandId(), cSerdes.commandResponse(), aSerdes.request()));


        // Get the stream of sequence numbers keyed by (aggregate key, sagaID)
        KStream<Tuple2<K, SagaId>, Long> snByAkSi = crArByCi
                .selectKey((k, v) -> Tuple2.of(v.v1().aggregateKey(), v.v2().sagaId))
                .mapValues(v -> v.v1().sequenceResult().getOrElse(Sequence.first()).getSeq());

        // Get the table of the largest sequence number keyed by (aggregate key, sagaID)
        return snByAkSi
                .groupByKey(Grouped.with(sagaAggKeySerde, Serdes.Long()))
                .reduce(reducer, Materialized.with(sagaAggKeySerde, Serdes.Long()));
    }

    /**
     * Receives command response from simplesourcing, and convert to simplesaga action response.
     */
    private static <A, D, K, C> KStream<SagaId, ActionResponse<A>> handleCommandResponse(
            EventSourcingContext<A, D, K, C> ctx,
            KStream<SagaId, ActionRequest<A>> actionRequests,
            KStream<CommandId, CommandResponse<K>> responseByCommandId) {
        Duration timeout = ctx.eventSourcingSpec.timeout;
        // find the response for the request
        KStream<CommandId, Tuple2<ActionRequest<A>, CommandResponse<K>>> actionRequestWithResponse =
                // join command response to action request by the command / action ID
                // TODO: timeouts - will be easy to do timeouts with a left join once https://issues.apache.org/jira/browse/KAFKA-6556 has been released
                actionRequests
                        .selectKey((k, aReq) -> aReq.actionCommand.commandId)
                        .join(
                                responseByCommandId,
                                Tuple2::of,
                                JoinWindows.of(timeout).until(timeout.toMillis() * 2 + 1),
                                Joined.with(ctx.cSerdes().commandId(), ctx.aSerdes().request(), ctx.cSerdes().commandResponse())
                        )
                        .peek(StreamUtils.logValues(logger, "joinActionRequestAndCommandResponse"));

        // turn the pair into an ActionResponse
        return actionRequestWithResponse
                .mapValues((k, v) -> {
                            ActionRequest<A> aReq = v.v1();
                            CommandResponse<K> cResp = v.v2();

                            // get the undo action if present
                            Optional<UndoCommand<A>> undoAction;
                            if (aReq.isUndo) {
                                undoAction = Optional.empty();
                            } else {
                                D decodedInput = getDecoded(ctx, aReq);
                                C command = ctx.eventSourcingSpec.commandMapper.apply(decodedInput);
                                K key = ctx.eventSourcingSpec.keyMapper.apply(decodedInput);
                                BiFunction<K, C, Optional<A>> undoFunction = ctx.eventSourcingSpec.undoCommand;
                                undoAction = undoFunction == null ?
                                        Optional.empty() :
                                        undoFunction.apply(key, command).map(undoA -> UndoCommand.of(undoA, aReq.actionCommand.actionType));
                            }

                            Result<CommandError, Sequence> sequenceResult =
                                    (cResp == null) ?
                                            Result.failure(CommandError.of(CommandError.Reason.Timeout,
                                                    "Timed out waiting for response from Command Processor")) :
                                            cResp.sequenceResult();
                            Result<SagaError, Optional<UndoCommand<A>>> result =
                                    sequenceResult.fold(errors -> {
                                                String message = String.join(",", errors.map(CommandError::getMessage));
                                                return Result.failure(SagaError.of(SagaError.Reason.CommandError, message));
                                            },
                                            seq -> Result.success(undoAction));

                            return ActionResponse.of(aReq.sagaId,
                                    aReq.actionId,
                                    aReq.actionCommand.commandId,
                                    aReq.isUndo,
                                    result);
                        }
                )
                .selectKey((k, resp) -> resp.sagaId)
                .peek(StreamUtils.logValues(logger, "resultStream"));
    }

}
