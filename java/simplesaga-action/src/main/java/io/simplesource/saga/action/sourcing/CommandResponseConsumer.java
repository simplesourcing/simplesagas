package io.simplesource.saga.action.sourcing;

import io.simplesource.kafka.api.CommandSerdes;
import io.simplesource.kafka.model.CommandResponse;
import io.simplesource.saga.action.common.Utils;
import io.simplesource.saga.shared.topics.TopicNamer;
import io.simplesource.saga.shared.topics.TopicTypes;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.StreamsBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class CommandResponseConsumer {
    static Logger logger = LoggerFactory.getLogger(CommandResponseConsumer.class);

    static <K, C> KStream<K, CommandResponse> commandResponseStream(CommandSerdes<K, C> commandSerdes,
                                                                    TopicNamer commandTopicNamer,
                                                                    StreamsBuilder builder) {
        return builder
                .stream(commandTopicNamer.apply(TopicTypes.CommandTopic.response),
                        Consumed.with(commandSerdes.aggregateKey(), commandSerdes.commandResponse()))
                .peek(Utils.logValues(logger, "commandResponseStream"));
    }
}
