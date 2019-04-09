package io.simplesource.saga.saga.app;


import io.simplesource.saga.model.saga.RetryStrategy;
import io.simplesource.saga.model.serdes.ActionSerdes;
import io.simplesource.saga.model.serdes.SagaSerdes;
import io.simplesource.saga.model.specs.ActionSpec;
import io.simplesource.saga.model.specs.SagaSpec;
import io.simplesource.saga.shared.topics.TopicNamer;
import lombok.Value;

import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;

@Value
public final class SagaContext<A> {
    public final SagaSerdes<A> sSerdes;
    public final ActionSerdes<A> aSerdes;
    public final SagaSpec<A> sSpec;
    public final ActionSpec<A> aSpec;
    public final TopicNamer sagaTopicNamer;
    public final Map<String, TopicNamer> actionTopicNamers;
    public final Map<String, RetryStrategy> retryStrategies;
    public final RetryPublisher<A> retryPublisher;
    public final ScheduledExecutorService executor;

    public SagaContext(SagaSpec<A> sSpec,
                       ActionSpec<A> aSpec,
                       TopicNamer sagaTopicNamer,
                       Map<String, TopicNamer> actionTopicNamers,
                       Map<String, RetryStrategy> retryStrategies,
                       RetryPublisher<A> retryPublisher,
                       ScheduledExecutorService executor) {

        this.sSpec = sSpec;
        this.aSpec = aSpec;
        this.sagaTopicNamer = sagaTopicNamer;
        this.actionTopicNamers = actionTopicNamers;
        this.retryStrategies = retryStrategies;
        this.retryPublisher = retryPublisher;
        this.executor = executor;
        sSerdes = sSpec.serdes;
        aSerdes = aSpec.serdes;
    }
}
