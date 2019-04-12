package io.simplesource.saga.action.eventsourcing;

import io.simplesource.kafka.api.CommandSerdes;
import io.simplesource.saga.action.internal.ActionContext;
import io.simplesource.saga.model.serdes.ActionSerdes;
import io.simplesource.saga.model.specs.ActionSpec;
import io.simplesource.saga.shared.topics.TopicNamer;
import lombok.Value;

/**
 * The type Event sourcing context. This is applied
 *
 * @param <A> - common representation form for all action commands (typically Json / GenericRecord for Avro)
 * @param <D> - intermediate decoded input type (that can easily be converted to both K and C)
 * @param <K> - aggregate key
 * @param <C> - simple sourcing command type
 */
@Value(staticConstructor = "of")
public final class EventSourcingContext<A, D, K, C> {
    public final ActionSpec<A> actionSpec;
    public final EventSourcingSpec<A, D, K, C> eventSourcingSpec;
    public final TopicNamer actionTopicNamer;
    public final TopicNamer commandTopicNamer;

    public ActionContext<A> getActionContext() {
        return new ActionContext<>(actionSpec, actionTopicNamer);
    }

    public final CommandSerdes<K, C> cSerdes() { return eventSourcingSpec.commandSerdes; }

    public final ActionSerdes<A> aSerdes() { return actionSpec.serdes; }
}
