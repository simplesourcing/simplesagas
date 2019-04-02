package io.simplesource.saga.action.internal;

import io.simplesource.saga.model.messages.ActionRequest;
import io.simplesource.saga.model.messages.ActionResponse;
import io.simplesource.saga.model.saga.SagaId;
import io.simplesource.saga.model.specs.ActionSpec;
import io.simplesource.saga.shared.streams.StreamUtils;
import io.simplesource.saga.shared.topics.TopicNamer;
import io.simplesource.saga.shared.topics.TopicTypes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class ActionConsumer {
    private static Logger logger = LoggerFactory.getLogger(ActionConsumer.class);

    static <A> KStream<SagaId, ActionRequest<A>> actionRequestStream(ActionSpec<A> spec,
                                                                     TopicNamer actionTopicNamer,
                                                                     StreamsBuilder builder) {
        String actionRequestTopic = actionTopicNamer.apply(TopicTypes.ActionTopic.ACTION_REQUEST);
        return builder.stream(
                actionRequestTopic,
                Consumed.with(spec.serdes.sagaId(), spec.serdes.request())
        ).peek(StreamUtils.logValues(logger, "actionRequestStream"));
    }

    static <A> KStream<SagaId, ActionResponse<A>> actionResponseStream(ActionSpec<A> spec,
                                                                    TopicNamer actionTopicNamer,
                                                                    StreamsBuilder builder) {
        String actionResponseTopic = actionTopicNamer.apply(TopicTypes.ActionTopic.ACTION_RESPONSE);
        return builder.stream(
                actionResponseTopic,
                Consumed.with(spec.serdes.sagaId(), spec.serdes.response())
        ).peek(StreamUtils.logValues(logger, "actionResponseStream"));
    }
}
