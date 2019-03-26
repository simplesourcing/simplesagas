package io.simplesource.saga.saga.app;


import io.simplesource.saga.model.serdes.ActionSerdes;
import io.simplesource.saga.model.serdes.SagaSerdes;
import io.simplesource.saga.model.specs.ActionProcessorSpec;
import io.simplesource.saga.model.specs.SagaSpec;
import io.simplesource.saga.shared.topics.TopicNamer;
import lombok.Value;

@Value
public final class SagaContext<A> {
    public final SagaSerdes<A> sSerdes;
    public final ActionSerdes<A> aSerdes;
    public final SagaSpec<A> sSpec;
    public final ActionProcessorSpec<A> aSpec;
    public final TopicNamer sagaTopicNamer;
    public final TopicNamer actionTopicNamer;

    public SagaContext(SagaSpec<A> sSpec,
            ActionProcessorSpec<A> aSpec,
            TopicNamer sagaTopicNamer,
            TopicNamer actionTopicNamer) {

        this.sSpec = sSpec;
        this.aSpec = aSpec;
        this.sagaTopicNamer = sagaTopicNamer;
        this.actionTopicNamer = actionTopicNamer;
        sSerdes = sSpec.serdes;
        aSerdes = aSpec.serdes;
    }
}
