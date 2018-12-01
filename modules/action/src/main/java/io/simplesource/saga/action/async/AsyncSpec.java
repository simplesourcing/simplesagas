package io.simplesource.saga.action.async;

import io.simplesource.data.Result;
import lombok.Value;

import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.Function;

/**
  * @param <A> - common representation form for all action commands (typically Json / GenericRecord for Avro)
  * @param <I> - input to async function
  * @param <K> - key for the output topic
  * @param <O> - output returned by async function
  * @param <R> - final result type that ends up in output topic
  */
@Value
final public class AsyncSpec<A, I, K, O, R> {
    public final String actionType;
    public final Function<A, Result<Throwable, I>> inputDecoder;
    public final Function<I, K> keyMapper;
    public final BiConsumer<I, CallBack<O>> asyncFunction;
    public final String groupId;
    public final Optional<AsyncOutput<I, K, O, R>> outputSpec;
}
