package io.simplesource.saga.action.internal;

import io.simplesource.saga.model.messages.ActionRequest;
import io.simplesource.saga.model.messages.ActionResponse;
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
import java.util.UUID;

public class AbstractActionTopologyBuilder<A> implements TopologyBuilder {

    private final ActionProcessorSpec<A> actionSpec;
    private final TopicConfig actionTopicConfig;
    private List<TopologyBuildListener<A>> buildListeners = new ArrayList<>();

    /**
     * Topology being built.
     */
    @Value
    public static class ActionTopologyContext<A> {
        StreamsBuilder builder;
        KStream<UUID, ActionRequest<A>> actionRequests;
        KStream<UUID, ActionResponse> actionResponses;
        Properties properties;
    }

    /**
     * Called when the topology is built, ie. to allow sub-topologies to be added.
     */
    public interface TopologyBuildListener<A> {

        void onBuildTopology(ActionTopologyContext<A> topologyContext);
    }

    AbstractActionTopologyBuilder(ActionProcessorSpec<A> actionSpec, TopicConfig actionTopicConfig) {
        this.actionSpec = actionSpec;
        this.actionTopicConfig = actionTopicConfig;
    }

    /**
     * Register a listener to be called when the topology is built, ie. to allow sub-topologies to be added.
     * @param listener to register.
     */
    public void onBuildTopology(TopologyBuildListener<A> listener) {
        buildListeners.add(listener);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Topology build(StreamAppConfig config) {
        Properties properties = StreamAppConfig.getConfig(config);
        StreamsBuilder builder = new StreamsBuilder();

        KStream<UUID, ActionRequest<A>> actionRequests =
                ActionConsumer.actionRequestStream(actionSpec, actionTopicConfig.namer, builder);
        KStream<UUID, ActionResponse> actionResponses =
                ActionConsumer.actionResponseStream(actionSpec, actionTopicConfig.namer, builder);

        ActionTopologyContext<A> topologyContext = new ActionTopologyContext<>(builder, actionRequests, actionResponses, properties);
        for (TopologyBuildListener<A> listener : buildListeners) {
            listener.onBuildTopology(topologyContext);
        }

        return builder.build();
    }
}
