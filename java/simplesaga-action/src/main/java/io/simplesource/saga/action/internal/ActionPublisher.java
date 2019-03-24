package io.simplesource.saga.action.internal;

import io.simplesource.saga.model.messages.ActionRequest;
import io.simplesource.saga.model.messages.ActionResponse;
import io.simplesource.saga.model.saga.SagaId;
import io.simplesource.saga.shared.topics.TopicTypes;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

final class ActionPublisher {

    static <A> void publishActionResponse(ActionContext<A> ctx, KStream<SagaId, ActionResponse> actionResponseStream) {
        actionResponseStream.to(
                ctx.actionTopicNamer.apply(TopicTypes.ActionTopic.response),
                Produced.with(ctx.actionSpec.serdes.sagaId(), ctx.actionSpec.serdes.response()));
    }

    static <A> void publishActionRequest(ActionContext<A> ctx, KStream<SagaId, ActionRequest<A>> request, boolean unprocessed) {
        request.to(ctx.actionTopicNamer.apply(unprocessed ? TopicTypes.ActionTopic.requestUnprocessed : TopicTypes.ActionTopic.request),
                Produced.with(ctx.actionSpec.serdes.sagaId(), ctx.actionSpec.serdes.request()));
    }
}
