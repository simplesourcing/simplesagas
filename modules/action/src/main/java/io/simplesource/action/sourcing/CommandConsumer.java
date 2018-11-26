package io.simplesource.action.sourcing

import io.simplesource.kafka.model.CommandResponse
import shared.topics.TopicNamer;
import lombok.val;
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.kstream.{Consumed, KStream}
import shared.topics.{TopicNamer, TopicTypes}

class CommandConsumer {

  static <A, I, K, C> KStream<K, CommandResponse> commandResponseStream(CommandSpec<A, I, K, C> spec,
                                                                        TopicNamer commandTopicNamer,
                                                                        StreamsBuilder builder) = {
    val responseByAggregate = builder
      .stream(commandTopicNamer(TopicTypes.CommandTopic.response),
                                  Consumed.`with`(spec.serdes.aggregateKey, spec.serdes.commandResponse()))
      .peek(SourcingStream.logValues[K, CommandResponse]("commandResponseStream"))
    responseByAggregate
  }
}
