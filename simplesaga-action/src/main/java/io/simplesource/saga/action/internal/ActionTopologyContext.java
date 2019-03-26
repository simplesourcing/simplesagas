package io.simplesource.saga.action.internal;

import io.simplesource.saga.model.messages.ActionRequest;
import io.simplesource.saga.model.messages.ActionResponse;
import io.simplesource.saga.model.saga.SagaId;
import io.simplesource.saga.model.specs.ActionProcessorSpec;
import io.simplesource.saga.shared.topics.TopicConfig;
import io.simplesource.saga.shared.topics.TopicNamer;
import lombok.Value;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

/**
 * Topology being built.
 */
@Value
public final class ActionTopologyContext<A> {
    public final StreamsBuilder builder;
    public final KStream<SagaId, ActionRequest<A>> actionRequests;
    public final KStream<SagaId, ActionResponse> actionResponses;
    public final Properties properties;

    public static <A> ActionTopologyContext<A> of(
            ActionProcessorSpec<A> actionSpec, TopicNamer actionTopicNamer, Properties config, StreamsBuilder builder) {
        KStream<SagaId, ActionRequest<A>> actionRequests =
                ActionConsumer.actionRequestStream(actionSpec, actionTopicNamer, builder);
        KStream<SagaId, ActionResponse> actionResponses =
                ActionConsumer.actionResponseStream(actionSpec, actionTopicNamer, builder);

        return new ActionTopologyContext<>(builder, actionRequests, actionResponses, config);
    }
}
