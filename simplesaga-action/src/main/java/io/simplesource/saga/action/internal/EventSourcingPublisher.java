package io.simplesource.saga.action.internal;

import io.simplesource.kafka.model.CommandRequest;
import io.simplesource.saga.action.eventsourcing.EventSourcingContext;
import io.simplesource.saga.shared.topics.TopicTypes;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

final class EventSourcingPublisher {

    static <A, D, K, C> void publishCommandRequest(EventSourcingContext<A, D, K, C> ctx,
                                                   KStream<K, CommandRequest<K, C>> commandRequestStream) {
        // publish to command request topic
        commandRequestStream.to(
                ctx.commandTopicNamer.apply(TopicTypes.CommandTopic.COMMAND_REQUEST),
                Produced.with(ctx.cSerdes().aggregateKey(), ctx.cSerdes().commandRequest())
        );
    }
}
