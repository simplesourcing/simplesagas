package io.simplesource.saga.action.internal;

import io.simplesource.kafka.model.CommandResponse;
import io.simplesource.saga.action.sourcing.SourcingContext;
import io.simplesource.saga.model.specs.ActionProcessorSpec;
import io.simplesource.saga.shared.topics.TopicConfig;
import org.apache.kafka.streams.kstream.KStream;

/**
 * Topology builder for SourcingApp.
 */
public final class SourcingTopologyBuilder<A> extends AbstractActionTopologyBuilder<A> {

    public SourcingTopologyBuilder(ActionProcessorSpec<A> actionSpec, TopicConfig actionTopicConfig) {
        super(actionSpec, actionTopicConfig);
    }

    /**
     * Add a sub-topology, ie for handling a Simple Sourcing command.
     */
    public <I, K, C> void addSubTopology(ActionTopologyContext<A> topologyContext,
                                         SourcingContext<A, I, K, C> sourcing) {
        KStream<K, CommandResponse> commandResponseStream = CommandConsumer.commandResponseStream(
                sourcing.commandSpec(), sourcing.commandTopicNamer(), topologyContext.builder());
        SourcingStream.addSubTopology(sourcing, topologyContext.actionRequests(), topologyContext.actionResponses(), commandResponseStream);
    }
}
