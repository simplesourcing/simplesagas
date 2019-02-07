package io.simplesource.saga.action.sourcing;

import io.simplesource.saga.model.serdes.ActionSerdes;
import io.simplesource.saga.shared.topics.TopicNamer;
import lombok.Value;

/**
 * @param <A> - common representation form for all action commands (typically Json / GenericRecord for Avro)
 */
@Value
public class ActionContext<A> {
    public final ActionSerdes<A> actionSerdes;
    public final TopicNamer actionTopicNamer;
}
