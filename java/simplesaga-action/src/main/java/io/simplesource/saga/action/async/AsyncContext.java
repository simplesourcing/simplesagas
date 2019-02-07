package io.simplesource.saga.action.async;

import io.simplesource.saga.model.serdes.ActionSerdes;
import io.simplesource.saga.shared.topics.TopicNamer;
import lombok.Value;

import java.util.concurrent.ScheduledExecutorService;

/**
  * @param <A> - common representation form for all action commands (typically Json / or GenericRecord for Avro)
  * @param <I> - input to async function
  * @param <K> - key for the output topic
  * @param <O> - output returned by async function
  * @param <R> - final result type that ends up in output topic
  */
@Value
public final class AsyncContext<A, I, K, O, R> {
    public final ActionSerdes<A> actionSerdes;
    public final TopicNamer actionTopicNamer;
    public final AsyncSpec<A, I, K, O, R> asyncSpec;
    public final ScheduledExecutorService executor;
}
