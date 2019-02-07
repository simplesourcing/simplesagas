package io.simplesource.saga.action.sourcing;

import io.simplesource.kafka.api.CommandSerdes;
import io.simplesource.saga.shared.topics.TopicNamer;
import lombok.Builder;
import lombok.Value;

import java.util.ArrayList;
import java.util.List;

/**
 * Configures a simple sourcing command topic, and the saga actions it handles.
 * @param <A> - saga action type (usually Json or GenericRecord for avro).
 * @param <K> - aggregate key.
 * @param <C> - simple sourcing command type.
 */
@Value
@Builder
public final class CommandSpec<A, K, C> {
    public final CommandSerdes<K, C> commandSerdes;
    public final TopicNamer commandTopicNamer;
    public final String aggregateName;
    public final long timeOutMillis;
    public final List<ActionCommandMapping<A, ?, K, C>> actions = new ArrayList<>();

    public <I> CommandSpec<A, K, C> handleAction(ActionCommandMapping<A, I, K, C> action) {
        this.actions.add(action);
        return this;
    }
}
