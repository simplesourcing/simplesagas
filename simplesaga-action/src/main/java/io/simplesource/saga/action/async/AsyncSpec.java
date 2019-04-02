package io.simplesource.saga.action.async;

import io.simplesource.data.Result;
import io.simplesource.saga.model.messages.UndoCommand;
import lombok.*;

import java.time.Duration;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.Function;

/**
  * @param <A> - common representation form for all action commands (typically Json / GenericRecord for Avro)
 * @param  <D> - intermediate decoded input type
  * @param <K> - key for the value topic
  * @param <O> - value returned by async function
  * @param <R> - final result type that ends up in value topic
  */
@Value
@Builder
@AllArgsConstructor(staticName = "of")
public final class AsyncSpec<A, D, K, O, R> {

    public final String actionType;
    public final Function<A, Result<Throwable, D>> inputDecoder;
    public final BiConsumer<D, Callback<O>> asyncFunction;
    public final String groupId;
    public final Optional<AsyncResult<A, D, K, O, R>> resultSpec;
    public final Optional<Duration> timeout;
}
