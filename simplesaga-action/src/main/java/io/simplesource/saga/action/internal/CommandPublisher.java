package io.simplesource.saga.action.internal;

import io.simplesource.kafka.model.CommandRequest;
import io.simplesource.saga.action.sourcing.SourcingContext;
import io.simplesource.saga.shared.topics.TopicTypes;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

class CommandPublisher {

    static <A, D, K, C> void publishCommandRequest(SourcingContext<A, D, K, C> ctx,
                                                   KStream<K, CommandRequest<K, C>> commandRequestStream) {
        // publish to command request topic
        commandRequestStream.to(
                ctx.commandTopicNamer.apply(TopicTypes.CommandTopic.request),
                Produced.with(ctx.cSerdes().aggregateKey(), ctx.cSerdes().commandRequest())
        );
    }
}
