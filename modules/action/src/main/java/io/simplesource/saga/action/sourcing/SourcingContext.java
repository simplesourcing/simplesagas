package io.simplesource.saga.action.sourcing;

import io.simplesource.kafka.api.CommandSerdes;
import io.simplesource.saga.model.serdes.ActionSerdes;
import io.simplesource.saga.model.specs.ActionProcessorSpec;
import io.simplesource.saga.shared.topics.TopicNamer;
import lombok.Value;

/**
  * @param <A> - common representation form for io.simplesource.io.simplesource.saga.user.saga.user.all action commands (typically Json / GenericRecord for Avro)
  * @param <I> - intermediate decoded input type (that can easily be converted to both K and C)
  * @param <K> - aggregate key
  * @param <C> - simple sourcing io.simplesource.io.simplesource.saga.user.saga.user.command type
  */

@Value
public class SourcingContext<A, I, K, C> {
    public final ActionProcessorSpec<A> actionSpec;
    public final CommandSpec<A, I, K, C> commandSpec;
    public final TopicNamer actionTopicNamer;
    public final TopicNamer commandTopicNamer;

    public final CommandSerdes<K, C> cSerdes() { return commandSpec.commandSerdes; }
    public final ActionSerdes<A> aSerdes() { return actionSpec.serdes; }
}
