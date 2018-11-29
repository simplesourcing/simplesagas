package io.simplesource.saga.action.async;

import lombok.Value;
import org.apache.kafka.common.serialization.Serde;

@Value
final public class AsyncSerdes<K, R>{
    final public Serde<K> key;
    final public Serde<R> output;
}
