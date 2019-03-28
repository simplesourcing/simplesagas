package io.simplesource.saga.action.async;

import io.simplesource.saga.action.app.ActionProcessor;
import io.simplesource.saga.shared.streams.StreamBuildSpec;
import io.simplesource.saga.action.internal.*;
import io.simplesource.saga.model.specs.ActionSpec;
import io.simplesource.saga.shared.topics.*;

import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

public final class AsyncBuilder {

    public static <A, D, K, O, R> ActionProcessor<A> apply(
            AsyncSpec<A, D, K, O, R> spec,
            TopicConfigBuilder.BuildSteps topicBuildFn,
            ScheduledExecutorService executor) {
        return streamBuildContext -> {
            ActionSpec<A> actionSpec = streamBuildContext.actionSpec;

            List<String> expectedTopicList = new ArrayList<>(TopicTypes.ActionTopic.all);
            expectedTopicList.add(TopicTypes.ActionTopic.ACTION_REQUEST_UNPROCESSED);

            TopicConfig actionTopicConfig = TopicConfigBuilder.build(
                    expectedTopicList,
                    topicBuildFn.withInitialStep(builder -> builder.withTopicBaseName(TopicUtils.actionTopicBaseName(spec.actionType))));

            List<TopicCreation> topics = actionTopicConfig.allTopics();
            spec.outputSpec.ifPresent(oSpec -> topics.addAll(oSpec.topicCreations));

            return new StreamBuildSpec(topics, builder -> {

                ActionTopologyContext<A> topologyContext = ActionTopologyContext.of(actionSpec, actionTopicConfig.namer, streamBuildContext.properties, builder);

                ScheduledExecutorService usedExecutor = executor != null ? executor : Executors.newScheduledThreadPool(1);
                AsyncContext<A, D, K, O, R> asyncContext = new AsyncContext<>(actionSpec, actionTopicConfig.namer, spec, usedExecutor);

                AsyncPipe pipe = AsyncStream.addSubTopology(topologyContext, asyncContext);
                return Optional.of(() -> {
                    if (executor == null) usedExecutor.shutdown();
                    pipe.close();
                });
            });
        };
    }

    public static <A, D, K, O, R> ActionProcessor<A> apply(
            AsyncSpec<A, D, K, O, R> spec,
            TopicConfigBuilder.BuildSteps topicBuildFn) {
        return apply(spec, topicBuildFn, null);
    }

    public static <A, D, K, O, R> ActionProcessor<A> apply(
            AsyncSpec<A, D, K, O, R> spec) {
        return apply(spec, a -> a, null);
    }

    public static <A, D, K, O, R> ActionProcessor<A> apply(
            AsyncSpec<A, D, K, O, R> spec,
            ScheduledExecutorService executor) {
        return apply(spec, a -> a, executor);
    }
}
