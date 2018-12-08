package io.simplesource.saga.action.sourcing;

import io.simplesource.api.CommandError;
import io.simplesource.data.NonEmptyList;
import io.simplesource.data.Result;
import io.simplesource.data.Sequence;
import io.simplesource.kafka.internal.util.Tuple2;
import io.simplesource.kafka.model.CommandRequest;
import io.simplesource.kafka.model.CommandResponse;
import io.simplesource.saga.action.common.ActionProducer;
import io.simplesource.saga.action.common.IdempotentStream;
import io.simplesource.saga.action.common.Utils;
import io.simplesource.saga.model.messages.ActionRequest;
import io.simplesource.saga.model.messages.ActionResponse;
import io.simplesource.saga.model.saga.SagaError;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;
import java.util.function.Function;


public class SourcingStream {
    static Logger logger = LoggerFactory.getLogger(SourcingStream.class);

    public static <K, V> ForeachAction<K, V> logValues(String prefix) {
        return (k, v) -> Utils.logValues(logger, prefix);
    }

    public static <A, I, K, C> void addSubTopology(SourcingContext<A, I, K, C> ctx,
                                                   KStream<UUID, ActionRequest<A>> actionRequest,
                                                   KStream<UUID, ActionResponse> actionResponse,
                                                   KStream<K, CommandResponse> commandResponseByAggregate) {
        KStream<UUID, CommandResponse> commandResponseByCommandId = commandResponseByAggregate.selectKey((k, v) -> v.commandId());

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

        // publish to output topics
        CommandProducer.commandRequest(ctx.commandSpec, ctx.commandTopicNamer, commandRequests);
        ActionProducer.actionResponse(ctx.actionSpec,
                ctx.actionTopicNamer,
                idempotentAction.priorResponses,
                newActionResponses,
                requestErrorResponses);
    }


    public static <A, I, K, C>
    Tuple2<KStream<UUID, ActionResponse>, KStream<K, CommandRequest<K, C>>> handleActionRequest(
            SourcingContext<A, I, K, C> ctx,
            KStream<UUID, ActionRequest<A>> actionRequests,
            KStream<K, CommandResponse> commandResponseByAggregate) {

        Function<CommandResponse, Sequence> getAggregateSequence = cResp ->
                cResp.sequenceResult().getOrElse(cResp.readSequence());

        KStream<UUID, Tuple2<ActionRequest<A>, Result<Throwable, I>>> reqsWithDecoded =
                actionRequests.mapValues((k, ar) -> Tuple2.of(ar, ctx.commandSpec.decode.apply(ar.actionCommand.command)));

        KStream<UUID, Tuple2<ActionRequest<A>, Result<Throwable, I>>>[] branchSuccessFailure = reqsWithDecoded.branch((k, v) -> v.v2().isSuccess(), (k, v) -> v.v2().isFailure());

        KStream<UUID, ActionResponse> errorActionResponses = branchSuccessFailure[1].mapValues((k, v) -> {
            ActionRequest<A> request = v.v1();
            NonEmptyList<Throwable> reasons = v.v2().failureReasons().get();
            return new ActionResponse(request.sagaId, request.actionId, request.actionCommand.commandId, Result.failure(
                    SagaError.of(SagaError.Reason.InternalError, reasons.head())));
        });

        KStream<UUID, Tuple2<ActionRequest<A>, I>> allGood = reqsWithDecoded.mapValues((k, v) -> Tuple2.of(v.v1(), v.v2().getOrElse(null)));

        // Sort incoming request by the aggregate key
        KStream<K, ActionRequest<A>> requestByAggregateKey = allGood
                .map((k, v) -> KeyValue.pair(ctx.commandSpec.keyMapper.apply(v.v2()), v.v1()))
                .peek(logValues("requestByAggregateKey"));

        Materialized<K, CommandResponse, KeyValueStore<Bytes, byte[]>> materializer =
                Materialized
                        .<K, CommandResponse, KeyValueStore<Bytes, byte[]>>as(
                                "last_command_by_aggregate_" + ctx.commandSpec.aggregateName)
                        .withKeySerde(ctx.cSerdes().aggregateKey())
                        .withValueSerde(ctx.cSerdes().commandResponse());

        // Get the most recent command response for the aggregate
        KTable<K, CommandResponse> lastCommandByAggregate =
                commandResponseByAggregate
                        .groupByKey()
                        .reduce((cr1, cr2) ->
                                        getAggregateSequence.apply(cr2).isGreaterThan(getAggregateSequence.apply(cr1)) ? cr2 : cr1,
                                materializer);

        ValueJoiner<ActionRequest<A>, CommandResponse, CommandRequest<K, C>> valueJoiner =
                (aReq, cResp) -> {
                    Sequence sequence = (cResp == null) ? Sequence.first() : getAggregateSequence.apply(cResp);

                    // we can do this safely as we have (unfortunately) already done this, and succeeded first time
                    I intermediate = ctx.commandSpec.decode.apply(aReq.actionCommand.command).getOrElse(null);
                    return new CommandRequest<>(ctx.commandSpec.keyMapper.apply(intermediate),
                            ctx.commandSpec.commandMapper.apply(intermediate),
                            sequence,
                            aReq.actionCommand.commandId);
                };

        // Get the latest sequence number and turn action request into a command request
        KStream<K, CommandRequest<K, C>> commandRequestByAggregate = requestByAggregateKey
                .leftJoin(
                        lastCommandByAggregate,
                        valueJoiner,
                        Joined.with(ctx.cSerdes().aggregateKey(),
                                ctx.aSerdes().request(),
                                ctx.cSerdes().commandResponse()))
                .peek(logValues("commandRequestByAggregate"));

        return Tuple2.of(errorActionResponses, commandRequestByAggregate);
    }


    public static <A, I, K, C> KStream<UUID, ActionResponse> handleCommandResponse(
            SourcingContext<A, I, K, C> ctx,
            KStream<UUID, ActionRequest<A>> actionRequests,
            KStream<UUID, CommandResponse> responseByCommandId) {
        long timeOutMillis = ctx.commandSpec.timeOutMillis;
        // find the response for the request
        KStream<UUID, Tuple2<ActionRequest<A>, CommandResponse>> actionRequestWithResponse =

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
                        .peek(logValues("handleCommandResponse"));


        // turn the pair into an ActionResponse
        return getActionResponse(ctx, actionRequestWithResponse);

    }

    static <A, I, K, C> KStream<UUID, ActionResponse> getActionResponse(
            SourcingContext<A, I, K, C> ctx,
            KStream<UUID, Tuple2<ActionRequest<A>, CommandResponse>> actionRequestWithResponse) {

        return actionRequestWithResponse
                .mapValues((k, v) -> {
                            ActionRequest<A> aReq = v.v1();
                            CommandResponse cResp = v.v2();
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
                .peek(logValues("resultStream"));
    }

}
