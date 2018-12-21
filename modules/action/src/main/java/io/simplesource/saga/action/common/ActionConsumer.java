package io.simplesource.saga.action.common;

import java.util.UUID;

import io.simplesource.saga.action.sourcing.SourcingStream;
import io.simplesource.saga.model.messages.ActionRequest;
import io.simplesource.saga.model.messages.ActionResponse;
import io.simplesource.saga.model.specs.ActionProcessorSpec;
import io.simplesource.saga.shared.topics.TopicNamer;
import io.simplesource.saga.shared.topics.TopicTypes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ActionConsumer {
    static Logger logger = LoggerFactory.getLogger(ActionConsumer.class);

    public static <A> KStream<UUID, ActionRequest<A>> actionRequestStream(
            ActionProcessorSpec<A> spec,
            TopicNamer actionTopicNamer,
            StreamsBuilder builder) {
        return builder.stream(
                actionTopicNamer.apply(TopicTypes.ActionTopic.request),
                Consumed.with(spec.serdes.uuid(), spec.serdes.request())
        ).peek(Utils.logValues(logger, "actionRequestStream"));
    }

    public static <A> KStream<UUID, ActionResponse> actionResponseStream(ActionProcessorSpec<A> spec,
                                                                  TopicNamer actionTopicNamer,
                                                                  StreamsBuilder builder) {
        return builder.stream(
                actionTopicNamer.apply(TopicTypes.ActionTopic.response),
                Consumed.with(spec.serdes.uuid(), spec.serdes.response())
        ).peek(Utils.logValues(logger, "actionResponseStream"));
    }
}
