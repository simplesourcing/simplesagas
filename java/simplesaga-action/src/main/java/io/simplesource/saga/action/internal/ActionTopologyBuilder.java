package io.simplesource.saga.action.internal;

import io.simplesource.saga.model.messages.ActionRequest;
import io.simplesource.saga.model.messages.ActionResponse;
import io.simplesource.saga.model.saga.SagaId;
import io.simplesource.saga.model.specs.ActionProcessorSpec;
import io.simplesource.saga.shared.topics.TopicConfig;
import io.simplesource.saga.shared.utils.StreamAppConfig;
import lombok.Value;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.function.Consumer;

public final class ActionTopologyBuilder<A> implements TopologyBuilder {

    private final ActionProcessorSpec<A> actionSpec;
    private final TopicConfig actionTopicConfig;
    private final List<Consumer<ActionTopologyContext<A>>> onBuildConsumers = new ArrayList<>();

    /**
     * Topology being built.
     */
    @Value
    public final static class ActionTopologyContext<A> {
        public final StreamsBuilder builder;
        public final KStream<SagaId, ActionRequest<A>> actionRequests;
        public final KStream<SagaId, ActionResponse> actionResponses;
        public final Properties properties;
    }

    public ActionTopologyBuilder(ActionProcessorSpec<A> actionSpec, TopicConfig actionTopicConfig) {
        this.actionSpec = actionSpec;
        this.actionTopicConfig = actionTopicConfig;
    }

    /**
     * Register a consumer to be called when the topology is built, ie. to allow sub-topologies to be added.
     * @param consumer to register.
     */
    public void onBuildTopology(Consumer<ActionTopologyContext<A>> consumer) {
        onBuildConsumers.add(consumer);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Topology build(StreamAppConfig config) {
        Properties properties = StreamAppConfig.getConfig(config);
        StreamsBuilder builder = new StreamsBuilder();

        KStream<SagaId, ActionRequest<A>> actionRequests =
                ActionConsumer.actionRequestStream(actionSpec, actionTopicConfig.namer, builder);
        KStream<SagaId, ActionResponse> actionResponses =
                ActionConsumer.actionResponseStream(actionSpec, actionTopicConfig.namer, builder);

        ActionTopologyContext<A> topologyContext = new ActionTopologyContext<>(builder, actionRequests, actionResponses, properties);
        for (Consumer<ActionTopologyContext<A>> consumer : onBuildConsumers) {
            consumer.accept(topologyContext);
        }

        return builder.build();
    }
}
