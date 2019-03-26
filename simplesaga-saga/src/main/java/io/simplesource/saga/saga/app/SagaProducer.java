package io.simplesource.saga.saga.app;

import io.simplesource.saga.model.messages.ActionRequest;
import io.simplesource.saga.model.messages.SagaResponse;
import io.simplesource.saga.model.messages.SagaStateTransition;
import io.simplesource.saga.model.saga.Saga;
import io.simplesource.saga.model.saga.SagaId;
import io.simplesource.saga.shared.topics.TopicTypes;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

final class SagaProducer {

    static <A> void publishActionRequests(SagaContext<A> ctx, KStream<SagaId, ActionRequest<A>> actionRequests) {
        actionRequests
                .to((x, actionRequest, z) -> {
                            String actionType = actionRequest.actionType();
                            String name = ctx.actionTopicNamer.apply(TopicTypes.ActionTopic.request + "-" + actionType.toLowerCase());
                            return name;
                    },
                        Produced.with(ctx.sSerdes.sagaId(), ctx.aSerdes.request()));
        actionRequests
                .to(ctx.actionTopicNamer.apply(TopicTypes.ActionTopic.request),
                        Produced.with(ctx.sSerdes.sagaId(), ctx.aSerdes.request()));
    }

    static <A> void publishSagaState(SagaContext<A> ctx, KStream<SagaId, Saga<A>> sagaState) {
        sagaState.to(ctx.sagaTopicNamer.apply(TopicTypes.SagaTopic.state),
                Produced.with(ctx.sSerdes.sagaId(), ctx.sSerdes.state()));
    }

    static <A> void publishSagaStateTransitions(SagaContext<A> ctx, KStream<SagaId, SagaStateTransition> transitions) {
        transitions.to(ctx.sagaTopicNamer.apply(TopicTypes.SagaTopic.stateTransition),
                Produced.with(ctx.sSerdes.sagaId(), ctx.sSerdes.transition()));
    }

    static <A> void publishSagaResponses(SagaContext<A> ctx, KStream<SagaId, SagaResponse> sagaResponse) {
        sagaResponse.to(ctx.sagaTopicNamer.apply(TopicTypes.SagaTopic.response),
                Produced.with(ctx.sSerdes.sagaId(), ctx.sSerdes.response()));
    }
}
