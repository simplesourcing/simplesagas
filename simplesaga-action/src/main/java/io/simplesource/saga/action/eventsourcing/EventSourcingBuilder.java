package io.simplesource.saga.action.eventsourcing;

import io.simplesource.saga.action.app.ActionProcessor;
import io.simplesource.saga.shared.streams.StreamBuildSpec;
import io.simplesource.saga.action.internal.*;
import io.simplesource.saga.model.specs.ActionSpec;
import io.simplesource.saga.shared.topics.TopicConfig;
import io.simplesource.saga.shared.topics.TopicConfigBuilder;
import io.simplesource.saga.shared.topics.TopicCreation;
import io.simplesource.saga.shared.topics.TopicTypes;

import java.util.List;
import java.util.Optional;

public final class EventSourcingBuilder {

    public static <A, D, K, C> ActionProcessor<A> apply(
            EventSourcingSpec<A, D, K, C> cSpec) {
        return apply(cSpec, a -> a, c -> c);
    }

    public static <A, D, K, C> ActionProcessor<A> apply(
            EventSourcingSpec<A, D, K, C> cSpec,
            TopicConfigBuilder.BuildSteps actionTopicBuilder,
            TopicConfigBuilder.BuildSteps commandTopicBuilder) {
        return streamBuildContext -> {
            ActionSpec<A> actionSpec = streamBuildContext.actionSpec;

            TopicConfig actionTopicConfig = TopicConfigBuilder.build(
                    TopicTypes.ActionTopic.all,
                    actionTopicBuilder.withInitialStep(builder -> builder.withTopicBaseName(cSpec.actionType.toLowerCase())));

            TopicConfig commandTopicConfig = TopicConfigBuilder.build(
                    TopicTypes.CommandTopic.all,
                    commandTopicBuilder.withInitialStep(builder -> builder.withTopicBaseName(cSpec.aggregateName.toLowerCase())));

            List<TopicCreation> topics = actionTopicConfig.allTopics();
            topics.addAll(commandTopicConfig.allTopics());

            return new StreamBuildSpec(topics, builder -> {
                EventSourcingContext<A, D, K, C> eventSourcingContext = EventSourcingContext.of(actionSpec, cSpec, actionTopicConfig.namer, commandTopicConfig.namer);
                ActionTopologyContext<A> topologyContext = ActionTopologyContext.of(actionSpec, actionTopicConfig.namer, streamBuildContext.properties, builder);
                EventSourcingStream.addSubTopology(topologyContext, eventSourcingContext);

                return Optional.empty();
            });
        };
    }
}
