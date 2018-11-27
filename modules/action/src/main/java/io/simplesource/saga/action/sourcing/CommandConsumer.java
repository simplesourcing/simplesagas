package io.simplesource.saga.action.sourcing;

import io.simplesource.kafka.model.CommandResponse;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import shared.topics.TopicNamer;
import org.apache.kafka.streams.StreamsBuilder;
import shared.topics.TopicTypes;

class CommandConsumer {
  static <A, I, K, C> KStream<K, CommandResponse> commandResponseStream(CommandSpec<A, I, K, C> spec,
                                                                        TopicNamer commandTopicNamer,
                                                                        StreamsBuilder builder) {
    KStream<K, CommandResponse> responseByAggregate = builder
            .stream(commandTopicNamer.apply(TopicTypes.CommandTopic.response()),
                    Consumed.with(spec.commandSerdes.aggregateKey(), spec.commandSerdes.commandResponse()))
            .peek(SourcingStream.logValues("commandResponseStream"));
    return responseByAggregate;
  }
}
