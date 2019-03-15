package io.simplesource.saga.action.internal;

import io.simplesource.saga.action.async.AsyncContext;
import io.simplesource.saga.model.specs.ActionProcessorSpec;
import io.simplesource.saga.shared.topics.TopicConfig;

import java.util.ArrayList;
import java.util.List;

/**
 * Topology builder for AsyncApp.
 */
public class AsyncTopologyBuilder<A> extends AbstractActionTopologyBuilder<A> {

    private List<AsyncTransform.AsyncPipe> asyncPipes = new ArrayList<>();

    public AsyncTopologyBuilder(ActionProcessorSpec<A> actionSpec, TopicConfig actionTopicConfig) {
        super(actionSpec, actionTopicConfig);
    }

    /**
     * Add a sub-topology for invoking an async function for a given async action.
     */
    public <I, K, O, R> void addSubTopology(ActionTopologyContext<A> topologyContext,
                                            AsyncContext<A, I, K, O, R> async) {
        AsyncStream.addSubTopology(async, topologyContext.actionRequests(), topologyContext.actionResponses());
        // create a Kafka consumer that processes action requests
        asyncPipes.add(AsyncTransform.async(async, topologyContext.properties()));
    }

    /**
     * Close async resources such as consumers (to be called on app shutdown).
     */
    public void shutdownResources() {
        asyncPipes.forEach(AsyncTransform.AsyncPipe::close);
    }
}
