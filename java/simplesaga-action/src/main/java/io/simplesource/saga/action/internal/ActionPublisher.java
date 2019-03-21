package io.simplesource.saga.action.internal;

import io.simplesource.saga.model.messages.ActionRequest;
import io.simplesource.saga.model.messages.ActionResponse;
import io.simplesource.saga.shared.topics.TopicTypes;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

import java.util.UUID;

final class ActionPublisher {

    static <A> void publishActionResponse(ActionContext<A> ctx, KStream<UUID, ActionResponse> actionResponseStream) {
        actionResponseStream.to(
                ctx.actionTopicNamer.apply(TopicTypes.ActionTopic.response),
                Produced.with(ctx.actionSpec.serdes.uuid(), ctx.actionSpec.serdes.response()));
    }

    static <A> void publishActionRequest(ActionContext<A> ctx, KStream<UUID, ActionRequest<A>> request, boolean unprocessed) {
        request.to(ctx.actionTopicNamer.apply(unprocessed ? TopicTypes.ActionTopic.requestUnprocessed : TopicTypes.ActionTopic.request),
                Produced.with(ctx.actionSpec.serdes.uuid(), ctx.actionSpec.serdes.request()));
    }
}
