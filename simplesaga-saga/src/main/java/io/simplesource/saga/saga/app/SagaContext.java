package io.simplesource.saga.saga.app;


import io.simplesource.saga.model.serdes.ActionSerdes;
import io.simplesource.saga.model.serdes.SagaSerdes;
import io.simplesource.saga.model.specs.ActionSpec;
import io.simplesource.saga.model.specs.SagaSpec;
import io.simplesource.saga.shared.topics.TopicNamer;
import lombok.Value;

import java.util.Map;

@Value
public final class SagaContext<A> {
    public final SagaSerdes<A> sSerdes;
    public final ActionSerdes<A> aSerdes;
    public final SagaSpec<A> sSpec;
    public final ActionSpec<A> aSpec;
    public final TopicNamer sagaTopicNamer;
    public final Map<String, TopicNamer> actionTopicNamers;

    public SagaContext(SagaSpec<A> sSpec,
            ActionSpec<A> aSpec,
            TopicNamer sagaTopicNamer,
            Map<String, TopicNamer> actionTopicNamers) {

        this.sSpec = sSpec;
        this.aSpec = aSpec;
        this.sagaTopicNamer = sagaTopicNamer;
        this.actionTopicNamers = actionTopicNamers;
        sSerdes = sSpec.serdes;
        aSerdes = aSpec.serdes;
    }
}
