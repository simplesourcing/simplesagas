package io.simplesource.saga.action.async;

import io.simplesource.saga.action.internal.ActionContext;
import io.simplesource.saga.model.specs.ActionProcessorSpec;
import io.simplesource.saga.shared.topics.TopicNamer;
import lombok.Value;

import java.util.concurrent.ScheduledExecutorService;

/**
  * @param <A> - common representation form for all action commands (typically Json / or GenericRecord for Avro)
  * @param <D> - intermediate decoded input type
  * @param <K> - key for the value topic
  * @param <O> - value returned by async function
  * @param <R> - final result type that ends up in value topic
  */
@Value
public final class AsyncContext<A, D, K, O, R> {
    public final ActionProcessorSpec<A> actionSpec;
    public final TopicNamer actionTopicNamer;
    public final AsyncSpec<A, D, K, O, R> asyncSpec;
    public final ScheduledExecutorService executor;

    public ActionContext<A> getActionContext() {
        return new ActionContext<>(actionSpec, actionTopicNamer);
    }
}
