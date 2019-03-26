package io.simplesource.saga.action.sourcing;

import io.simplesource.saga.action.common.StreamApp;
import io.simplesource.saga.action.common.StreamBuildSpec;
import io.simplesource.saga.action.internal.*;
import io.simplesource.saga.model.specs.ActionProcessorSpec;
import io.simplesource.saga.shared.topics.TopicConfig;
import io.simplesource.saga.shared.topics.TopicConfigBuilder;
import io.simplesource.saga.shared.topics.TopicCreation;
import io.simplesource.saga.shared.topics.TopicTypes;
import io.simplesource.saga.shared.utils.StreamAppUtils;
import org.apache.kafka.streams.StreamsBuilder;

import java.util.Collections;
import java.util.List;
import java.util.function.Function;

public final class SourcingBuilder {

    public static <A, D, K, C> StreamApp.BuildStep<ActionProcessorSpec<A>> sourcingSteps(CommandSpec<A, D, K, C> cSpec, TopicConfigBuilder.BuildSteps actionTopicBuilder, TopicConfigBuilder.BuildSteps commandTopicBuilder) {
        return topologyBuildContext -> {
            ActionProcessorSpec<A> actionSpec = topologyBuildContext.buildInput;

            TopicConfig actionTopicConfig = TopicConfigBuilder.buildTopics(TopicTypes.ActionTopic.all, Collections.emptyMap(), Collections.emptyMap(), actionTopicBuilder);

            List<TopicCreation> topics = TopicCreation.allTopics(actionTopicConfig);
            TopicConfig commandTopicConfig = TopicConfigBuilder.buildTopics(TopicTypes.CommandTopic.all, Collections.emptyMap(), Collections.emptyMap(), commandTopicBuilder);
            topics.addAll(TopicCreation.allTopics(commandTopicConfig));

            Function<StreamsBuilder, StreamAppUtils.ShutdownHandler> topologyBuildStep = builder -> {
                SourcingContext<A, D, K, C> sourcingContext = new SourcingContext<>(actionSpec, cSpec, actionTopicConfig.namer, commandTopicConfig.namer);

                ActionTopologyContext<A> topologyContext = ActionTopologyContext.of(actionSpec, actionTopicConfig, builder, topologyBuildContext.config);
                SourcingStream.addSubTopology(topologyContext, sourcingContext);

                return null;
            };

            return new StreamBuildSpec(topics, topologyBuildStep);
        };
    }
}
