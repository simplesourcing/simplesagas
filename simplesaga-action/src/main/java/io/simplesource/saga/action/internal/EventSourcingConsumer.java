package io.simplesource.saga.action.internal;

import io.simplesource.kafka.model.CommandResponse;
import io.simplesource.saga.action.eventsourcing.EventSourcingSpec;
import io.simplesource.saga.shared.topics.TopicNamer;
import io.simplesource.saga.shared.topics.TopicTypes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class EventSourcingConsumer {
    private static Logger logger = LoggerFactory.getLogger(EventSourcingConsumer.class);

    static <A, D, K, C> KStream<K, CommandResponse<K>> commandResponseStream(
            EventSourcingSpec<A, D, K, C> spec, TopicNamer commandTopicNamer, StreamsBuilder builder) {
        return builder
                .stream(commandTopicNamer.apply(TopicTypes.CommandTopic.COMMAND_RESPONSE),
                        Consumed.with(spec.commandSerdes.aggregateKey(), spec.commandSerdes.commandResponse()))
                .peek(StreamUtils.logValues(logger, "commandResponseStream"));
    }
}
