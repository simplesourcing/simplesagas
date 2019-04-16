package io.simplesource.saga.saga.internal;

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
                            String actionType = actionRequest.actionCommand.actionType();
                            return ctx.actionTopicNamers.get(actionType.toLowerCase()).apply(TopicTypes.ActionTopic.ACTION_REQUEST);
                    },
                    Produced.with(ctx.sSerdes.sagaId(), ctx.aSerdes.request()));
    }

    static <A> void publishSagaState(SagaContext<A> ctx, KStream<SagaId, Saga<A>> sagaState) {
        sagaState.to(ctx.sagaTopicNamer.apply(TopicTypes.SagaTopic.SAGA_STATE),
                Produced.with(ctx.sSerdes.sagaId(), ctx.sSerdes.state()));
    }

    static <A> void publishSagaStateTransitions(SagaContext<A> ctx, KStream<SagaId, SagaStateTransition<A>> transitions) {
        transitions.to(ctx.sagaTopicNamer.apply(TopicTypes.SagaTopic.SAGA_STATE_TRANSITION),
                Produced.with(ctx.sSerdes.sagaId(), ctx.sSerdes.transition()));
    }

    static <A> void publishSagaResponses(SagaContext<A> ctx, KStream<SagaId, SagaResponse> sagaResponse) {
        sagaResponse.to(ctx.sagaTopicNamer.apply(TopicTypes.SagaTopic.SAGA_RESPONSE),
                Produced.with(ctx.sSerdes.sagaId(), ctx.sSerdes.response()));
    }
}
