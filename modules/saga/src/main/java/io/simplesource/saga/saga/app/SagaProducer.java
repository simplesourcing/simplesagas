package io.simplesource.saga.saga.app;

import java.util.UUID;

import io.simplesource.saga.model.messages.ActionRequest;
import io.simplesource.saga.model.messages.SagaResponse;
import io.simplesource.saga.model.messages.SagaStateTransition;
import io.simplesource.saga.model.saga.Saga;
import io.simplesource.saga.shared.topics.TopicTypes;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;


public final class SagaProducer {

    public static <A> void actionRequests(SagaContext<A> ctx, KStream<UUID, ActionRequest<A>> actionRequests) {
        actionRequests
                .to(ctx.actionTopicNamer.apply(TopicTypes.ActionTopic.request),
                        Produced.with(ctx.aSerdes.uuid(), ctx.aSerdes.request()));
    }

    public static <A> void sagaState(SagaContext<A> ctx, KStream<UUID, Saga<A>> sagaState) {
        sagaState.to(ctx.sagaTopicNamer.apply(TopicTypes.SagaTopic.state),
                Produced.with(ctx.sSerdes.uuid(), ctx.sSerdes.state()));
    }

    public static <A> void sagaStateTransitions(SagaContext<A> ctx, KStream<UUID, SagaStateTransition<A>>... transitions) {
        for (KStream<UUID, SagaStateTransition<A>> t : transitions) {
            t.to(ctx.sagaTopicNamer.apply(TopicTypes.SagaTopic.stateTransition),
                    Produced.with(ctx.sSerdes.uuid(), ctx.sSerdes.transition()));
        }
    }

    public static <A> void sagaResponses(SagaContext<A> ctx, KStream<UUID, SagaResponse> sagaResponse) {
        sagaResponse.to(ctx.sagaTopicNamer.apply(TopicTypes.SagaTopic.response),
                Produced.with(ctx.sSerdes.uuid(), ctx.sSerdes.response()));
    }
}
