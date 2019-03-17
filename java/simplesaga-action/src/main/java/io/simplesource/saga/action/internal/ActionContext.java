package io.simplesource.saga.action.internal;

import io.simplesource.saga.model.specs.ActionProcessorSpec;
import io.simplesource.saga.shared.topics.TopicNamer;
import lombok.Value;

@Value
public class ActionContext<A> {
    final ActionProcessorSpec<A> actionSpec;
    final TopicNamer actionTopicNamer;
}
