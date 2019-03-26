package io.simplesource.saga.action.async;

import io.simplesource.saga.shared.streams.StreamApp;
import io.simplesource.saga.shared.streams.StreamBuildSpec;
import io.simplesource.saga.action.internal.*;
import io.simplesource.saga.model.specs.ActionProcessorSpec;
import io.simplesource.saga.shared.topics.TopicConfig;
import io.simplesource.saga.shared.topics.TopicConfigBuilder;
import io.simplesource.saga.shared.topics.TopicCreation;
import io.simplesource.saga.shared.topics.TopicTypes;
import io.simplesource.saga.shared.streams.StreamAppUtils;
import org.apache.kafka.streams.StreamsBuilder;

import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Function;

public final class AsyncBuilder {

    public static <A, D, K, O, R> StreamApp.BuildStep<ActionProcessorSpec<A>> apply(
            AsyncSpec<A, D, K, O, R> spec,
            TopicConfigBuilder.BuildSteps topicBuildFn) {
        return apply(spec, topicBuildFn, null);
    }

    public static <A, D, K, O, R> StreamApp.BuildStep<ActionProcessorSpec<A>> apply(
            AsyncSpec<A, D, K, O, R> spec,
            TopicConfigBuilder.BuildSteps topicBuildFn,
            ScheduledExecutorService executor) {
        return topologyBuildContext -> {
            ActionProcessorSpec<A> actionSpec = topologyBuildContext.buildInput;

            List<String> expectedTopicList = new ArrayList<>(TopicTypes.ActionTopic.all);
            expectedTopicList.add(TopicTypes.ActionTopic.requestUnprocessed);

            TopicConfig actionTopicConfig = TopicConfigBuilder.buildTopics(expectedTopicList, topicBuildFn);

            List<TopicCreation> topics = TopicCreation.allTopics(actionTopicConfig);

            Function<StreamsBuilder, StreamAppUtils.ShutdownHandler> topologyBuildStep = builder -> {
                ScheduledExecutorService usedExecutor = executor != null ? executor : Executors.newScheduledThreadPool(1);

                ActionTopologyContext<A> topologyContext = ActionTopologyContext.of(actionSpec, actionTopicConfig, builder, topologyBuildContext.config);
                AsyncContext<A, D, K, O, R> asyncContext = new AsyncContext<>(actionSpec, actionTopicConfig.namer, spec, usedExecutor);

                AsyncPipe pipe = AsyncStream.addSubTopology(topologyContext, asyncContext);
                return () -> {
                    if (executor == null) usedExecutor.shutdown();
                    pipe.close();
                };
            };

            return new StreamBuildSpec(topics, topologyBuildStep);
        };
    }
}
