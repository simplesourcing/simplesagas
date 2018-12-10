package io.simplesource.saga.saga.app;
//
//import java.util.UUID
//
//import model.messages.{ActionResponse, SagaRequest, SagaResponse, SagaStateTransition}
//import model.saga.Saga
//import model.specs.{ActionProcessorSpec, SagaSpec}
//import org.apache.kafka.streams.StreamsBuilder
//import org.apache.kafka.streams.kstream.{Consumed, KStream}
//import org.slf4j.LoggerFactory
//import shared.topics.TopicNamer
//import shared.topics.TopicTypes.{ActionTopic, SagaTopic}


import io.simplesource.saga.model.messages.ActionResponse;
import io.simplesource.saga.model.messages.SagaRequest;
import io.simplesource.saga.model.messages.SagaResponse;
import io.simplesource.saga.model.messages.SagaStateTransition;
import io.simplesource.saga.model.saga.Saga;
import io.simplesource.saga.model.specs.ActionProcessorSpec;
import io.simplesource.saga.model.specs.SagaSpec;
import io.simplesource.saga.shared.topics.TopicNamer;
import io.simplesource.saga.shared.topics.TopicTypes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public final class SagaConsumer {

    public static <A> KStream<UUID, SagaRequest<A>> sagaRequest(SagaSpec<A> spec,
                                                                TopicNamer sagaTopicNamer,
                                                                StreamsBuilder builder) {
        return builder.stream(sagaTopicNamer.apply(TopicTypes.SagaTopic.request),
                Consumed.with(spec.serdes.uuid(), spec.serdes.request()));
    }

    public static <A> KStream<UUID, SagaResponse> sagaResponse(SagaSpec<A> spec,
                                                               TopicNamer sagaTopicNamer,
                                                               StreamsBuilder builder) {
        return builder.stream(sagaTopicNamer.apply(TopicTypes.SagaTopic.response),
                Consumed.with(spec.serdes.uuid(), spec.serdes.response()));
    }

    public static <A> KStream<UUID, SagaStateTransition> stateTransition(SagaSpec<A> spec,
                                                                            TopicNamer sagaTopicNamer,
                                                                            StreamsBuilder builder) {
        return builder.stream(sagaTopicNamer.apply(TopicTypes.SagaTopic.stateTransition),
                Consumed.with(spec.serdes.uuid(), spec.serdes.transition()));
    }

    public static <A> KStream<UUID, Saga<A>> state(SagaSpec<A> spec,
                                                   TopicNamer sagaTopicNamer,
                                                   StreamsBuilder builder) {
        return builder.stream(sagaTopicNamer.apply(TopicTypes.SagaTopic.state),
                Consumed.with(spec.serdes.uuid(), spec.serdes.state()));
    }

    public static <A> KStream<UUID, ActionResponse> actionResponse(ActionProcessorSpec<A> actionSpec,
                                                                   TopicNamer topicNamer,
                                                                   StreamsBuilder builder) {
        return builder.stream(topicNamer.apply(TopicTypes.ActionTopic.response),
                Consumed.with(actionSpec.serdes.uuid(), actionSpec.serdes.response()));
    }
}
