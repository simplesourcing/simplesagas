package io.simplesource.saga.action.common;

import java.util.UUID;

import io.simplesource.saga.model.messages.ActionRequest;
import io.simplesource.saga.model.messages.ActionResponse;
import io.simplesource.saga.shared.topics.TopicNamer;
import io.simplesource.saga.shared.topics.TopicTypes;
import model.specs.ActionProcessorSpec;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;


class ActionConsumer {
    static <A> KStream<UUID, ActionRequest<A>> actionRequestStream(
            ActionProcessorSpec<A> spec,
            TopicNamer actionTopicNamer,
            StreamsBuilder builder) {
        KStream<UUID, ActionRequest<A>> a = builder.stream(
                actionTopicNamer.apply(TopicTypes.ActionTopic.request),
                Consumed.with(spec.serdes().uuid(), spec.serdes().request())
        );
        return a;
    }

    static <A> KStream<UUID, ActionResponse> actionResponseStream(ActionProcessorSpec<A> spec,
                                                                  TopicNamer actionTopicNamer,
                                                                  StreamsBuilder builder) {
        return builder.stream(
                actionTopicNamer.apply(TopicTypes.ActionTopic.response),
                Consumed.with(spec.serdes().uuid(), spec.serdes().response())
        );
    }
}
