package io.simplesource.saga.action.async;

import io.simplesource.saga.action.app.ActionProcessorBuildStep;
import io.simplesource.saga.shared.app.StreamAppUtils;
import io.simplesource.saga.shared.app.StreamBuildSpec;
import io.simplesource.saga.action.internal.*;
import io.simplesource.saga.model.specs.ActionSpec;
import io.simplesource.saga.shared.topics.*;

import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 * A class with a single static function that returns an action processor build step for an async action.
 * <p>
 * An async action is an action that executes an arbitrary asynchronous function in a saga. The result of the function can be
 * optionally piped to an output topic.
 * <p>
 * This build step:
 * <ol>
 * <li>Defines the stream topology for the Async processor</li>
 * <li>Creates producers and consumers that handle the asynchronous invocation</li>
 * <li>Resolves the topic configuration (names and configuration properties)</li>
 * <li>Handles cleans up of resources when the stream terminates</li>
 * </ol>
 */
public final class AsyncBuilder {

    /**
     * @param <A> common representation form for all action commands (typically Json or GenericRecord/SpecificRecord for Avro)
     * @param <D> intermediate decoded input type
     * @param <K> key for the output topic (if the result of async invocation is written to an output topic)
     * @param <O> output value returned by async function
     * @param <R> final result type that ends up in output topic
     * @param spec         a data structure with all the details required to set up the async invocation and process the results
     * @param topicBuildFn a functional interface representing topic configuration steps
     * @param executor     the executor service that is used to invoke asynchronously and schedule timeouts
     * @return the action processor build step
     */
    public static <A, D, K, O, R> ActionProcessorBuildStep<A> apply(
            AsyncSpec<A, D, K, O, R> spec,
            TopicConfigBuilder.BuildSteps topicBuildFn,
            ScheduledExecutorService executor) {
        return streamBuildContext -> {
            ActionSpec<A> actionSpec = streamBuildContext.actionSpec;

            List<String> expectedTopicList = new ArrayList<>(TopicTypes.ActionTopic.all);

            spec.resultSpec.ifPresent(rSpec ->
                        expectedTopicList.add(TopicTypes.ActionTopic.ACTION_OUTPUT));

            expectedTopicList.add(TopicTypes.ActionTopic.ACTION_REQUEST_UNPROCESSED);

            TopicConfig actionTopicConfig = topicBuildFn
                    .withInitialStep(builder -> builder.withTopicBaseName(TopicUtils.actionTopicBaseName(spec.actionType)))
                    .build(expectedTopicList);

            List<TopicCreation> topics = actionTopicConfig.allTopics();

            return new StreamBuildSpec(topics, builder -> {

                ActionTopologyContext<A> topologyContext = ActionTopologyContext.of(
                        actionSpec,
                        actionTopicConfig.namer,
                        streamBuildContext.propertiesBuilder,
                        builder);

                ScheduledExecutorService usedExecutor = executor != null ? executor : Executors.newScheduledThreadPool(1);
                AsyncContext<A, D, K, O, R> asyncContext = new AsyncContext<>(actionSpec, actionTopicConfig.namer, spec, usedExecutor);

                AsyncPipe pipe = AsyncStream.addSubTopology(topologyContext, asyncContext);
                return Optional.of(() -> {
                    if (executor == null) StreamAppUtils.shutdownExecutorService(usedExecutor);
                    pipe.close();
                });
            });
        };
    }

    public static <A, D, K, O, R> ActionProcessorBuildStep<A> apply(
            AsyncSpec<A, D, K, O, R> spec,
            TopicConfigBuilder.BuildSteps topicBuildFn) {
        return apply(spec, topicBuildFn, null);
    }

    public static <A, D, K, O, R> ActionProcessorBuildStep<A> apply(
            AsyncSpec<A, D, K, O, R> spec) {
        return apply(spec, a -> a, null);
    }

    public static <A, D, K, O, R> ActionProcessorBuildStep<A> apply(
            AsyncSpec<A, D, K, O, R> spec,
            ScheduledExecutorService executor) {
        return apply(spec, a -> a, executor);
    }
}
