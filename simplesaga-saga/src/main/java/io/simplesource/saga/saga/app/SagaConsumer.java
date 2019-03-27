package io.simplesource.saga.saga.app;

import io.simplesource.saga.model.messages.ActionResponse;
import io.simplesource.saga.model.messages.SagaRequest;
import io.simplesource.saga.model.messages.SagaResponse;
import io.simplesource.saga.model.messages.SagaStateTransition;
import io.simplesource.saga.model.saga.Saga;
import io.simplesource.saga.model.saga.SagaId;
import io.simplesource.saga.model.specs.ActionProcessorSpec;
import io.simplesource.saga.model.specs.SagaSpec;
import io.simplesource.saga.shared.topics.TopicNamer;
import io.simplesource.saga.shared.topics.TopicTypes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

final class SagaConsumer {

    static <A> KStream<SagaId, SagaRequest<A>> sagaRequest(SagaSpec<A> spec,
                                                         TopicNamer sagaTopicNamer,
                                                         StreamsBuilder builder) {
        return builder.stream(sagaTopicNamer.apply(TopicTypes.SagaTopic.SAGA_REQUEST),
                Consumed.with(spec.serdes.sagaId(), spec.serdes.request()));
    }

    static <A> KStream<SagaId, SagaResponse> sagaResponse(SagaSpec<A> spec,
                                                        TopicNamer sagaTopicNamer,
                                                        StreamsBuilder builder) {
        return builder.stream(sagaTopicNamer.apply(TopicTypes.SagaTopic.SAGA_RESPONSE),
                Consumed.with(spec.serdes.sagaId(), spec.serdes.response()));
    }

    static <A> KStream<SagaId, SagaStateTransition> stateTransition(SagaSpec<A> spec,
                                                                    TopicNamer sagaTopicNamer,
                                                                    StreamsBuilder builder) {
        return builder.stream(sagaTopicNamer.apply(TopicTypes.SagaTopic.SAGA_STATE_TRANSITION),
                Consumed.with(spec.serdes.sagaId(), spec.serdes.transition()));
    }

    static <A> KStream<SagaId, Saga<A>> state(SagaSpec<A> spec,
                                            TopicNamer sagaTopicNamer,
                                            StreamsBuilder builder) {
        return builder.stream(sagaTopicNamer.apply(TopicTypes.SagaTopic.SAGA_STATE),
                Consumed.with(spec.serdes.sagaId(), spec.serdes.state()));
    }

    static <A> KStream<SagaId, ActionResponse> actionResponse(ActionProcessorSpec<A> actionSpec,
                                                            Map<String, TopicNamer> topicNamers,
                                                            StreamsBuilder builder) {
        List<String> actionResponseNames = topicNamers.values()
                .stream()
                .map(tn -> tn.apply(TopicTypes.ActionTopic.ACTION_RESPONSE))
                .collect(Collectors.toList());

        return builder.stream(actionResponseNames,
                Consumed.with(actionSpec.serdes.sagaId(), actionSpec.serdes.response()));
    }
}
