package io.simplesource.saga.action.async;

import io.simplesource.saga.action.internal.ActionContext;
import io.simplesource.saga.model.specs.ActionSpec;
import io.simplesource.saga.shared.topics.TopicNamer;
import lombok.Value;

import java.util.concurrent.ScheduledExecutorService;

/**
  * @param <A> common representation form for all action commands (typically Json or GenericRecord/SpecificRecord for Avro)
  * @param <D> intermediate decoded input type (this can be specific to this action processor)
  * @param <K> key for the output topic (if the result of async invocation is written to an output topic)
  * @param <O> output value returned by async function
  * @param <R> final result type that ends up in output topic (if output is generated)
  */
@Value
public final class AsyncContext<A, D, K, O, R> {
    public final ActionSpec<A> actionSpec;
    public final TopicNamer actionTopicNamer;
    public final AsyncSpec<A, D, K, O, R> asyncSpec;
    public final ScheduledExecutorService executor;

    public ActionContext<A> getActionContext() {
        return new ActionContext<>(actionSpec, actionTopicNamer);
    }
}
