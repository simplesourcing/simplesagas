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
            SourcingSpec<A, D, K, C> cSpec,
            TopicConfigBuilder.BuildSteps actionTopicBuilder,
            TopicConfigBuilder.BuildSteps commandTopicBuilder) {
        return streamBuildContext -> {
            ActionProcessorSpec<A> actionSpec = streamBuildContext.appInput;

            TopicConfig actionTopicConfig = TopicConfigBuilder.build(
                    TopicTypes.ActionTopic.all,
                    actionTopicBuilder.withInitialStep(builder -> builder.withTopicBaseName(cSpec.actionType.toLowerCase())));

            TopicConfig commandTopicConfig = TopicConfigBuilder.build(
                    TopicTypes.CommandTopic.all,
                    commandTopicBuilder.withInitialStep(builder -> builder.withTopicBaseName(cSpec.aggregateName.toLowerCase())));

            List<TopicCreation> topics = actionTopicConfig.allTopics();
            topics.addAll(commandTopicConfig.allTopics());

            return new StreamBuildSpec(topics, builder -> {
                SourcingContext<A, D, K, C> sourcingContext = SourcingContext.of(actionSpec, cSpec, actionTopicConfig.namer, commandTopicConfig.namer);
                ActionTopologyContext<A> topologyContext = ActionTopologyContext.of(actionSpec, actionTopicConfig.namer, streamBuildContext.properties, builder);
                SourcingStream.addSubTopology(topologyContext, sourcingContext);

                return Optional.empty();
            });
        };
    }
}
