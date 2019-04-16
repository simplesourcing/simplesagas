package io.simplesource.saga.action.eventsourcing;

import io.simplesource.saga.action.app.ActionProcessorBuildStep;
import io.simplesource.saga.shared.app.StreamBuildSpec;
import io.simplesource.saga.action.internal.*;
import io.simplesource.saga.model.specs.ActionSpec;
import io.simplesource.saga.shared.topics.*;

import java.util.List;
import java.util.Optional;

/**
 * A class with a single static function that returns an action processor build step for a simple sourcing event sourcing action.
 * <p>
 * An event sourcing action is an action that is processed as a simple sourcing command against an aggregate.
 * <p>
 * Simple sourcing records the version number of an aggregate, called a sequence number, and every successful command results in this sequence number being incremented.
 * The event sourcing action processor will ensure that only the saga can write to the aggregate, and if any other writes to the aggregate occur during the saga, the saga will fail.
 * An optimistic lock is essentially held on the aggregate for the duration of the saga.
 */
public final class EventSourcingBuilder {

    /**
     * A static function that returns an action processor build step that:
     * 1.   Defines the stream topology for an EventSourcing processor
     * 2.   Computes topic configuration (names and configuration properties) for the action processor. This includes both the action request and response topics, and the command request and response topics.
     *
     * @param <A>                 - common representation form for all action commands (typically Json / GenericRecord for Avro)
     * @param <D>                 - intermediate decoded input type (that can easily be converted to both K and C)
     * @param <K>                 - aggregate key type
     * @param <C>                 - simple sourcing command type
     * @param esSpec              data structure specifying how to turn the action request into a command request and interpret the result
     * @param actionTopicBuilder  the action topic builder - a mechanism for configuring the action topics
     * @param commandTopicBuilder the command topic builder - a mechanism for configuring the simple sourcing command topics
     * @return the action processor build step
     */
    public static <A, D, K, C> ActionProcessorBuildStep<A> apply(
            EventSourcingSpec<A, D, K, C> esSpec,
            TopicConfigBuilder.BuildSteps actionTopicBuilder,
            TopicConfigBuilder.BuildSteps commandTopicBuilder) {
        return streamBuildContext -> {
            ActionSpec<A> actionSpec = streamBuildContext.actionSpec;

            TopicConfig actionTopicConfig = actionTopicBuilder
                    .withInitialStep(builder -> builder.withTopicBaseName(TopicUtils.actionTopicBaseName(esSpec.actionType)))
                    .build(TopicTypes.ActionTopic.all);

            TopicConfig commandTopicConfig = commandTopicBuilder
                    .withInitialStep(builder -> builder.withTopicBaseName(esSpec.aggregateName.toLowerCase()))
                    .build(TopicTypes.CommandTopic.all);

            List<TopicCreation> topics = actionTopicConfig.allTopics();
            topics.addAll(commandTopicConfig.allTopics());

            return new StreamBuildSpec(topics, builder -> {
                EventSourcingContext<A, D, K, C> eventSourcingContext = EventSourcingContext.of(actionSpec, esSpec, actionTopicConfig.namer, commandTopicConfig.namer);
                ActionTopologyContext<A> topologyContext = ActionTopologyContext.of(
                        actionSpec,
                        actionTopicConfig.namer,
                        streamBuildContext.propertiesBuilder,
                        builder);
                EventSourcingStream.addSubTopology(topologyContext, eventSourcingContext);

                return Optional.empty();
            });
        };
    }

    public static <A, D, K, C> ActionProcessorBuildStep<A> apply(
            EventSourcingSpec<A, D, K, C> esSpec) {
        return apply(esSpec, a -> a, c -> c);
    }
}
