package io.simplesource.saga.action.async;

import io.simplesource.data.Result;
import io.simplesource.saga.model.messages.UndoCommand;
import io.simplesource.saga.model.serdes.TopicSerdes;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Value;

import java.util.Optional;
import java.util.function.Function;

@Value(staticConstructor = "of")
@Builder
@AllArgsConstructor(staticName = "of")
public final class AsyncResult<A, D, K, O, R> {

    public interface UndoFunction<A, D, K, R> {
        Optional<UndoCommand<A>> apply(D decodedInput, K outputKey, R result);
    }

    public final Function<O, Optional<Result<Throwable, R>>> outputMapper;
    public final Function<D, K> keyMapper;
    public final UndoFunction<A, D, K, R> undoFunction;
    public final Optional<TopicSerdes<K, R>> outputSerdes;
}
