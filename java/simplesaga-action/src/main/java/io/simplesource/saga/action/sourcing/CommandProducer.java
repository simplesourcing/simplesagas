package io.simplesource.saga.action.sourcing;

import io.simplesource.kafka.model.CommandRequest;
import io.simplesource.saga.shared.topics.TopicTypes;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

public class CommandProducer {

    public static <A, K, C> void commandRequest(CommandSpec<A, K, C> cSpec,
                                             KStream<K, CommandRequest<K, C>> commandRequestByAggregate) {
        // publish to command request topic
        commandRequestByAggregate.to(
                cSpec.commandTopicNamer.apply(TopicTypes.CommandTopic.request),
                Produced.with(cSpec.commandSerdes.aggregateKey(), cSpec.commandSerdes.commandRequest())
        );
    }
}
