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
    String actionType;
    Function<A, Result<Throwable, I>> inputDecoder;
    Function<K, I> keyMapper;
    BiConsumer<I, CallBackProvider> asyncFunction;
    String groupId;
    Optional<AsyncOutput<I, K, O, R>> outputSpec;
}
