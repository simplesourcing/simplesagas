package io.simplesource.saga.action.internal;

import io.simplesource.saga.model.messages.ActionRequest;
import io.simplesource.saga.model.messages.ActionResponse;
import io.simplesource.saga.model.saga.SagaId;
import io.simplesource.saga.model.specs.ActionSpec;
import io.simplesource.saga.shared.properties.PropertiesBuilder;
import io.simplesource.saga.shared.topics.TopicNamer;
import lombok.Value;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;

/**
 * Topology being built.
 */
@Value
public final class ActionTopologyContext<A> {
    public final StreamsBuilder builder;
    public final KStream<SagaId, ActionRequest<A>> actionRequests;
    public final KStream<SagaId, ActionResponse<A>> actionResponses;
    public final PropertiesBuilder.BuildSteps properties;

    public static <A> ActionTopologyContext<A> of(
            ActionSpec<A> actionSpec, TopicNamer actionTopicNamer, PropertiesBuilder.BuildSteps config, StreamsBuilder builder) {
        KStream<SagaId, ActionRequest<A>> actionRequests =
                ActionConsumer.actionRequestStream(actionSpec, actionTopicNamer, builder);
        KStream<SagaId, ActionResponse<A>> actionResponses =
                ActionConsumer.actionResponseStream(actionSpec, actionTopicNamer, builder);

        return new ActionTopologyContext<>(builder, actionRequests, actionResponses, config);
    }
}
