package io.simplesource.saga.action.internal;

import io.simplesource.kafka.model.CommandResponse;
import io.simplesource.saga.action.sourcing.CommandSpec;
import io.simplesource.saga.shared.topics.TopicNamer;
import io.simplesource.saga.shared.topics.TopicTypes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class CommandConsumer {
    private static Logger logger = LoggerFactory.getLogger(CommandConsumer.class);

    static <A, I, K, C> KStream<K, CommandResponse> commandResponseStream(
            CommandSpec<A, I, K, C> spec, TopicNamer commandTopicNamer, StreamsBuilder builder) {
        return builder
                .stream(commandTopicNamer.apply(TopicTypes.CommandTopic.response),
                        Consumed.with(spec.commandSerdes.aggregateKey(), spec.commandSerdes.commandResponse()))
                .peek(Utils.logValues(logger, "commandResponseStream"));
    }
}
