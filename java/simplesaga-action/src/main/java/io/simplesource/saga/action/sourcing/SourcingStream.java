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


public class SourcingStream {
    static Logger logger = LoggerFactory.getLogger(SourcingStream.class);

    static <K, V> ForeachAction<K, V> logValues(String prefix) {
        return (k, v) -> logger.info("{}: {}={}", prefix, k.toString().substring(0, 6), v.toString());
    }

    /**
     * Add a sub-topology for a simple sourcing command topic, including all the saga actions that map to that topic.
     * @param actionContext action spec.
     * @param commandSpec command spec.
     * @param actionRequestKStream KStream of action requests (from saga coordinator).
     * @param actionResponseKStream KStream of action responses (from saga coordinator).
     * @param commandResponseKStream KStream of command responses (from simple sourcing).
     * @param <A>
     * @param <K>
     * @param <C>
     */
    static <A, K, C> void addSubTopology(
            ActionContext<A> actionContext,
            CommandSpec<A, K, C> commandSpec,
            KStream<UUID, ActionRequest<A>> actionRequestKStream,
            KStream<UUID, ActionResponse> actionResponseKStream,
            KStream<K, CommandResponse> commandResponseKStream) {

        KStream<UUID, CommandResponse> commandResponseByCommandId = commandResponseKStream.selectKey((k, v) -> v.commandId());

        Materialized<K, CommandResponse, KeyValueStore<Bytes, byte[]>> materializer =
                Materialized
                        .<K, CommandResponse, KeyValueStore<Bytes, byte[]>>as(
                                "last_command_by_aggregate_" + commandSpec.aggregateName)
                        .withKeySerde(commandSpec.commandSerdes.aggregateKey())
                        .withValueSerde(commandSpec.commandSerdes.commandResponse());

        // Get the most recent command response for the aggregate
        KTable<K, CommandResponse> lastCommandByAggregate =
                commandResponseKStream
                        .groupByKey()
                        .reduce((cr1, cr2) ->
                                        getAggregateSequence(cr2).isGreaterThan(getAggregateSequence(cr1)) ? cr2 : cr1,
                                materializer);

        for (ActionCommandMapping<A, ?, K, C> actionCommandMapping : commandSpec.actions) {
            // create kstream off the action topic for each action type
            IdempotentStream.IdempotentAction<A> idempotentAction = IdempotentStream.getActionRequestsWithResponse(
                    actionContext.actionSerdes, actionRequestKStream, actionResponseKStream, actionCommandMapping.actionType);

            // get new command requests
            Tuple2<KStream<UUID, ActionResponse>, KStream<K, CommandRequest<K, C>>> requestResp = actionToCommandRequest(
                    actionContext, commandSpec, actionCommandMapping, idempotentAction.unprocessedRequests, lastCommandByAggregate);

            KStream<UUID, ActionResponse> requestErrorResponses = requestResp.v1();
            KStream<K, CommandRequest<K, C>> commandRequests = requestResp.v2();

            // publish command requests to simplesource command topic
            CommandProducer.commandRequest(commandSpec, commandRequests);

            // publish prior responses and error responses directly to the simplesaga action response topic
            ActionProducer.actionResponse(actionContext.actionSerdes(), actionContext.actionTopicNamer(),
                    idempotentAction.processedResponses,
                    requestErrorResponses);
        }

        KStream<UUID, ActionResponse> newActionResponses = commandToActionResponse(actionContext, commandSpec, actionRequestKStream, commandResponseByCommandId);
        // publish command responses (from simplesourcing) to the simplesaga action response topic
        ActionProducer.actionResponse(actionContext.actionSerdes(), actionContext.actionTopicNamer(), newActionResponses);
    }

    /**
     * Translate simplesaga action requests to simplesourcing command requests.
     */
    static <A, I, K, C> Tuple2<KStream<UUID, ActionResponse>, KStream<K, CommandRequest<K, C>>> actionToCommandRequest(
            ActionContext<A> actionContext,
            CommandSpec<A, K, C> commandSpec,
            ActionCommandMapping<A, I, K, C> actionCommandMapping,
            KStream<UUID, ActionRequest<A>> actionRequestKStream,
            KTable<K, CommandResponse> lastCommandByAggregate) {

        KStream<UUID, Tuple2<ActionRequest<A>, Result<Throwable, I>>> reqsWithDecoded =
                actionRequestKStream
                        .mapValues((k, ar) -> Tuple2.of(ar, actionCommandMapping.decode.apply(ar.actionCommand.command)))
                        .peek(logValues("reqsWithDecoded"));

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
                .map((k, v) -> KeyValue.pair(actionCommandMapping.keyMapper.apply(v.v2()), v.v1()))
                .peek(logValues("requestByAggregateKey"));

        ValueJoiner<ActionRequest<A>, CommandResponse, CommandRequest<K, C>> valueJoiner =
                (aReq, cResp) -> {
                    Sequence sequence = (cResp == null) ? Sequence.first() : getAggregateSequence(cResp);

                    // we can do this safely as we have (unfortunately) already done this, and succeeded first time
                    I intermediate = actionCommandMapping.decode.apply(aReq.actionCommand.command).getOrElse(null);
                    return new CommandRequest<>(actionCommandMapping.keyMapper.apply(intermediate),
                            actionCommandMapping.commandMapper.apply(intermediate),
                            sequence,
                            aReq.actionCommand.commandId);
                };

        // Get the latest sequence number and turn action request into a command request
        KStream<K, CommandRequest<K, C>> commandRequestByAggregate = requestByAggregateKey
                .leftJoin(
                        lastCommandByAggregate,
                        valueJoiner,
                        Joined.with(commandSpec.commandSerdes.aggregateKey(),
                                actionContext.actionSerdes().request(),
                                commandSpec.commandSerdes.commandResponse()))
                .peek(logValues("commandRequestByAggregate"));

        return Tuple2.of(errorActionResponses, commandRequestByAggregate);
    }

    /**
     * Receives command response from simplesourcing, and convert to simplesaga action response.
     */
    static <A, K, C> KStream<UUID, ActionResponse> commandToActionResponse(
            ActionContext<A> actionContext,
            CommandSpec<A, K, C> commandSpec,
            KStream<UUID, ActionRequest<A>> actionRequests,
            KStream<UUID, CommandResponse> responseByCommandId) {
        long timeOutMillis = commandSpec.timeOutMillis;
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
                                Joined.with(actionContext.actionSerdes.uuid(), actionContext.actionSerdes().request(), commandSpec.commandSerdes.commandResponse())
                        )
                        .peek(logValues("commandToActionResponse"));


        // turn the pair into an ActionResponse
        return getActionResponse(actionRequestWithResponse);
    }

    static <A> KStream<UUID, ActionResponse> getActionResponse(
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

    private static Sequence getAggregateSequence(CommandResponse commandResponse) {
        return commandResponse.sequenceResult().getOrElse(commandResponse.readSequence());
    }
}
