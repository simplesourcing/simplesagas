package io.simplesource.saga.saga.app;

import io.simplesource.saga.model.messages.ActionResponse;
import io.simplesource.saga.model.messages.SagaRequest;
import io.simplesource.saga.model.messages.SagaResponse;
import io.simplesource.saga.model.messages.SagaStateTransition;
import io.simplesource.saga.model.saga.RetryStrategy;
import io.simplesource.saga.model.saga.Saga;
import io.simplesource.saga.model.saga.SagaId;
import io.simplesource.saga.model.specs.SagaSpec;
import io.simplesource.saga.shared.topics.TopicNamer;
import io.simplesource.saga.shared.topics.TopicTypes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class SagaTopologyBuilder {

    static class DelayedRetryPublisher<A> {

        private final ScheduledExecutorService executor;
        private final Map<String, RetryStrategy> retryStrategies;
        private final RetryPublisher publisher;
        private final String topicName;

        DelayedRetryPublisher(SagaContext<A> ctx) {
            executor = ctx.executor;
            retryStrategies =  ctx.retryStrategies;
            publisher = ctx.retryPublisher;
            topicName = ctx.sagaTopicNamer.apply(TopicTypes.SagaTopic.SAGA_STATE_TRANSITION);
        }

        void send(String actionType, int retryCount, SagaId sagaId, SagaStateTransition<A> transition) {
            if (retryStrategies == null) return;
            RetryStrategy strategy = retryStrategies.get(actionType);
            if (strategy == null) return;
            strategy.nextRetry(retryCount).ifPresent(duration -> {
                executor.schedule(() -> {
                    publisher.send(topicName, sagaId, transition);

                }, duration.toMillis(), TimeUnit.MILLISECONDS);
            });
        }
    }

    public static <A> void addSubTopology(SagaContext<A> sagaContext, StreamsBuilder builder) {
        // get input topic streams
        SagaSpec<A> sagaSpec = sagaContext.sSpec;
        TopicNamer sagaTopicNamer = sagaContext.sagaTopicNamer;
        DelayedRetryPublisher<A> retryPublisher = new DelayedRetryPublisher<>(sagaContext);

        KStream<SagaId, SagaRequest<A>> sagaRequest = SagaConsumer.sagaRequest(sagaSpec, sagaTopicNamer, builder);
        KStream<SagaId, Saga<A>> sagaState = SagaConsumer.state(sagaSpec, sagaTopicNamer, builder);
        KStream<SagaId, SagaStateTransition<A>> sagaStateTransition = SagaConsumer.stateTransition(sagaSpec, sagaTopicNamer, builder);

        KStream<SagaId, ActionResponse<A>> actionResponse = SagaConsumer.actionResponse(
                sagaContext.aSpec,
                sagaContext.actionTopicNamers,
                builder);

        SagaStream.addSubTopology(sagaContext, retryPublisher, sagaRequest, sagaStateTransition, sagaState, actionResponse);

        DistributorContext<SagaId, SagaResponse> distCtx = new DistributorContext<>(
                new DistributorSerdes<>(sagaSpec.serdes.sagaId(), sagaSpec.serdes.response()),
                sagaTopicNamer.apply(TopicTypes.SagaTopic.SAGA_RESPONSE_TOPIC_MAP),
                sagaSpec.responseWindow,
                response -> response.sagaId,
                key -> key.id);

        KStream<SagaId, String> topicNames = ResultDistributor.resultTopicMapStream(distCtx, builder);
        KStream<SagaId, SagaResponse> sagaResponse = SagaConsumer.sagaResponse(sagaSpec, sagaTopicNamer, builder);
        ResultDistributor.distribute(distCtx, sagaResponse, topicNames);
    }
}
