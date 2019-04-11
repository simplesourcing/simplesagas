package io.simplesource.saga.action.eventsourcing;

import io.simplesource.saga.action.app.ActionProcessorBuildStep;
import io.simplesource.saga.shared.streams.StreamBuildSpec;
import io.simplesource.saga.action.internal.*;
import io.simplesource.saga.model.specs.ActionSpec;
import io.simplesource.saga.shared.topics.*;

import java.util.List;
import java.util.Optional;

public final class EventSourcingBuilder {

    public static <A, D, K, C> ActionProcessorBuildStep<A> apply(
            EventSourcingSpec<A, D, K, C> esSpec,
            TopicConfigBuilder.BuildSteps actionTopicBuilder,
            TopicConfigBuilder.BuildSteps commandTopicBuilder) {
        return streamBuildContext -> {
            ActionSpec<A> actionSpec = streamBuildContext.actionSpec;

            TopicConfig actionTopicConfig = TopicConfigBuilder.build(
                    TopicTypes.ActionTopic.all,
                    actionTopicBuilder.withInitialStep(builder ->
                            builder.withTopicBaseName(TopicUtils.actionTopicBaseName(esSpec.actionType))));

            TopicConfig commandTopicConfig = TopicConfigBuilder.build(
                    TopicTypes.CommandTopic.all,
                    commandTopicBuilder.withInitialStep(builder ->
                            builder.withTopicBaseName(esSpec.aggregateName.toLowerCase())));

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
