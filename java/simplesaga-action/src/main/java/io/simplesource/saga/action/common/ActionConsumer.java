package io.simplesource.saga.action.common;

import io.simplesource.saga.model.messages.ActionRequest;
import io.simplesource.saga.model.messages.ActionResponse;
import io.simplesource.saga.model.serdes.ActionSerdes;
import io.simplesource.saga.shared.topics.TopicNamer;
import io.simplesource.saga.shared.topics.TopicTypes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class ActionConsumer {
    static Logger logger = LoggerFactory.getLogger(ActionConsumer.class);

    /**
     * Return a KStream of ActionRequests.
     */
    public static <A> KStream<UUID, ActionRequest<A>> actionRequestStream(
            ActionSerdes<A> actionSerdes,
            TopicNamer actionTopicNamer,
            StreamsBuilder builder) {
        return builder.stream(
                actionTopicNamer.apply(TopicTypes.ActionTopic.request),
                Consumed.with(actionSerdes.uuid(), actionSerdes.request())
        ).peek(Utils.logValues(logger, "actionRequestStream"));
    }

    /**
     * Return a KStream of ActionResponses.
     */
    public static <A> KStream<UUID, ActionResponse> actionResponseStream(
            ActionSerdes<A> actionSerdes,
            TopicNamer actionTopicNamer,
            StreamsBuilder builder) {
        return builder.stream(
                actionTopicNamer.apply(TopicTypes.ActionTopic.response),
                Consumed.with(actionSerdes.uuid(), actionSerdes.response())
        ).peek(Utils.logValues(logger, "actionResponseStream"));
    }
}
