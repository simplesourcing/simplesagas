package io.simplesource.saga.action.sourcing;

import io.simplesource.saga.shared.streams.StreamBuildSpec;
import io.simplesource.saga.action.internal.*;
import io.simplesource.saga.model.specs.ActionProcessorSpec;
import io.simplesource.saga.shared.streams.StreamBuildStep;
import io.simplesource.saga.shared.topics.TopicConfig;
import io.simplesource.saga.shared.topics.TopicConfigBuilder;
import io.simplesource.saga.shared.topics.TopicCreation;
import io.simplesource.saga.shared.topics.TopicTypes;

import java.util.List;
import java.util.Optional;

public final class SourcingBuilder {

    public static <A, D, K, C> StreamBuildStep<ActionProcessorSpec<A>> apply(
            CommandSpec<A, D, K, C> cSpec,
            TopicConfigBuilder.BuildSteps actionTopicBuilder,
            TopicConfigBuilder.BuildSteps commandTopicBuilder) {
        return streamBuildContext -> {
            ActionProcessorSpec<A> actionSpec = streamBuildContext.appInput;

            TopicConfig actionTopicConfig = TopicConfigBuilder.buildTopics(TopicTypes.ActionTopic.all, actionTopicBuilder);
            List<TopicCreation> topics = TopicCreation.allTopics(actionTopicConfig);

            TopicConfig commandTopicConfig = TopicConfigBuilder.buildTopics(TopicTypes.CommandTopic.all, commandTopicBuilder);
            topics.addAll(TopicCreation.allTopics(commandTopicConfig));

            return new StreamBuildSpec(topics, builder -> {
                SourcingContext<A, D, K, C> sourcingContext = SourcingContext.of(actionSpec, cSpec, actionTopicConfig.namer, commandTopicConfig.namer);
                ActionTopologyContext<A> topologyContext = ActionTopologyContext.of(actionSpec, actionTopicConfig.namer, streamBuildContext.properties, builder);
                SourcingStream.addSubTopology(topologyContext, sourcingContext);

                return Optional.empty();
            });
        };
    }
}
