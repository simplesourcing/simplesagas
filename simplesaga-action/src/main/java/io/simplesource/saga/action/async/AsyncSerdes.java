package io.simplesource.saga.action.async;

import lombok.Value;
import org.apache.kafka.common.serialization.Serde;

@Value
public final class AsyncSerdes<K, R>{
    public final Serde<K> key;
    public final Serde<R> output;
}
