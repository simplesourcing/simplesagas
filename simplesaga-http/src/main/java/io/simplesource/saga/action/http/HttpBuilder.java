package io.simplesource.saga.action.http;

import io.simplesource.saga.action.app.ActionProcessorBuildStep;
import io.simplesource.saga.action.async.AsyncBuilder;
import io.simplesource.saga.action.async.AsyncResult;
import io.simplesource.saga.action.async.AsyncSpec;
import io.simplesource.saga.shared.topics.TopicConfigBuilder;

import java.util.concurrent.ScheduledExecutorService;

// TODO: does this belong in userland?
public final class HttpBuilder {

    public static <A, K, B, O, R> ActionProcessorBuildStep<A> apply(
            HttpSpec<A, K, B, O, R> httpSpec,
            TopicConfigBuilder.BuildSteps topicBuildFn,
            ScheduledExecutorService executor) {

        AsyncSpec<A, HttpRequest<K, B>, K, O, R> asyncSpec =
                AsyncSpec.of(
                        httpSpec.actionType,
                        httpSpec.decoder::decode,
                        httpSpec.asyncHttpClient,
                        httpSpec.groupId,
                        httpSpec.outputSpec.map(o ->
                                AsyncResult.of(
                                        o.decoder::decode,
                                        r -> r.key,
                                        (request, k, r) -> o.undoFunction.apply(request, r),
                                        o.outputSerdes
                                )),
                        httpSpec.timeout);

        return AsyncBuilder.apply(asyncSpec, topicBuildFn, executor);
    }

    public static <A, D, K, O, R> ActionProcessorBuildStep<A> apply(
            HttpSpec<A, D, K, O, R> spec,
            TopicConfigBuilder.BuildSteps topicBuildFn) {
        return apply(spec, topicBuildFn, null);
    }

    public static <A, D, K, O, R> ActionProcessorBuildStep<A> apply(
            HttpSpec<A, D, K, O, R> spec) {
        return apply(spec, a -> a, null);
    }

    public static <A, D, K, O, R> ActionProcessorBuildStep<A> apply(
            HttpSpec<A, D, K, O, R> spec,
            ScheduledExecutorService executor) {
        return apply(spec, a -> a, executor);
    }
}


