package io.simplesource.saga.action.sourcing;

import io.simplesource.data.Result;
import lombok.Builder;
import lombok.Value;

import java.util.function.Function;

/**
 * Specifies a saga action type and how it should be decoded and mapped to a simple sourcing command.
 * @param <A> - common representation form for all action commands (typically Json / GenericRecord for Avro)
 * @param <I> - intermediate decoded input type (that can easily be converted both K and C)
 * @param <K> - aggregate key
 * @param <C> - simple sourcing command type
 */
@Value
@Builder
public class ActionCommandMapping<A, I, K, C> {
    public final String actionType;
    public final Function<A, Result<Throwable, I>> decode;
    public final Function<I, C> commandMapper;
    public final Function<I, K> keyMapper;
}
