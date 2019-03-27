package io.simplesource.saga.saga.app;

import io.simplesource.saga.model.messages.ActionResponse;
import io.simplesource.saga.model.messages.SagaRequest;
import io.simplesource.saga.model.messages.SagaResponse;
import io.simplesource.saga.model.messages.SagaStateTransition;
import io.simplesource.saga.model.saga.Saga;
import io.simplesource.saga.model.saga.SagaId;
import io.simplesource.saga.model.specs.SagaSpec;
import io.simplesource.saga.shared.topics.TopicNamer;
import io.simplesource.saga.shared.topics.TopicTypes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;

public class SagaTopologyBuilder {

    public static <A> void addSubTopology(SagaContext<A> sagaContext, StreamsBuilder builder) {
        // get input topic streams
        SagaSpec<A> sagaSpec = sagaContext.sSpec;
        TopicNamer sagaTopicNamer = sagaContext.sagaTopicNamer;

        KStream<SagaId, SagaRequest<A>> sagaRequest = SagaConsumer.sagaRequest(sagaSpec, sagaTopicNamer, builder);
        KStream<SagaId, Saga<A>> sagaState = SagaConsumer.state(sagaSpec, sagaTopicNamer, builder);
        KStream<SagaId, SagaStateTransition> sagaStateTransition = SagaConsumer.stateTransition(sagaSpec, sagaTopicNamer, builder);

        KStream<SagaId, ActionResponse> actionResponse = SagaConsumer.actionResponse(
                sagaContext.aSpec,
                sagaContext.actionTopicNamers,
                builder);

        SagaStream.addSubTopology(sagaContext, sagaRequest, sagaStateTransition, sagaState, actionResponse);

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
