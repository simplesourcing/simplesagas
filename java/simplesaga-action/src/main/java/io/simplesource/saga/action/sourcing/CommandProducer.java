package io.simplesource.saga.action.sourcing;

import io.simplesource.kafka.model.CommandRequest;
import io.simplesource.saga.shared.topics.TopicTypes;
import org.apache.kafka.streams.kstream.KStream;
import io.simplesource.saga.shared.topics.TopicNamer;
import org.apache.kafka.streams.kstream.Produced;

public class CommandProducer {

    public static <A, I, K, C> void commandRequest(CommandSpec<A, I, K, C> cSpec,
                                 TopicNamer commandTopicNamer,
                                 KStream<K, CommandRequest<K, C>> commandRequestByAggregate) {
    // publish to command request topic
    commandRequestByAggregate.to(
      commandTopicNamer.apply(TopicTypes.CommandTopic.request),
      Produced.with(cSpec.commandSerdes.aggregateKey(), cSpec.commandSerdes.commandRequest())
    );
  }
}
