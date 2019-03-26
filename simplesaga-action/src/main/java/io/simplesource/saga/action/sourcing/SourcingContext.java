package io.simplesource.saga.action.sourcing;

import io.simplesource.kafka.api.CommandSerdes;
import io.simplesource.saga.action.internal.ActionContext;
import io.simplesource.saga.model.serdes.ActionSerdes;
import io.simplesource.saga.model.specs.ActionProcessorSpec;
import io.simplesource.saga.shared.topics.TopicNamer;
import lombok.Value;

/**
  * @param <A> - common representation form for all action commands (typically Json / GenericRecord for Avro)
  * @param <D> - intermediate decoded input type (that can easily be converted to both K and C)
  * @param <K> - aggregate key
  * @param <C> - simple sourcing command type
  */

@Value(staticConstructor = "of")
public final class SourcingContext<A, D, K, C> {
    public final ActionProcessorSpec<A> actionSpec;
    public final SourcingSpec<A, D, K, C> commandSpec;
    public final TopicNamer actionTopicNamer;
    public final TopicNamer commandTopicNamer;

    public ActionContext<A> getActionContext() {
        return new ActionContext<>(actionSpec, actionTopicNamer);
    }

    public final CommandSerdes<K, C> cSerdes() { return commandSpec.commandSerdes; }
    public final ActionSerdes<A> aSerdes() { return actionSpec.serdes; }
}
