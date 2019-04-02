package io.simplesource.saga.action.eventsourcing;

import io.simplesource.data.Result;
import io.simplesource.data.Sequence;
import io.simplesource.kafka.api.CommandSerdes;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Value;

import java.time.Duration;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
  * @param <A> - common representation form for all action commands (typically Json / GenericRecord for Avro)
  * @param <D> - intermediate decoded input type (that can easily be converted both K and C)
  * @param <K> - aggregate key
  * @param <C> - simple sourcing command type
  */
@Value
@Builder
@AllArgsConstructor(staticName = "of")
public final class EventSourcingSpec<A, D, K, C> {
    public final String actionType;
    public final String aggregateName;
    public final Function<A, Result<Throwable, D>> decode;
    public final Function<D, C> commandMapper;
    public final Function<D, K> keyMapper;
    public final Function<D, Sequence> sequenceMapper;
    public final BiFunction<K, C, Optional<A>> undoCommand;
    public final CommandSerdes<K, C> commandSerdes;
    public final Duration timeout;
}
