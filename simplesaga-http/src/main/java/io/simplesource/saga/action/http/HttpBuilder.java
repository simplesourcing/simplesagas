package io.simplesource.saga.action.http;

import io.simplesource.saga.action.app.ActionProcessorBuildStep;
import io.simplesource.saga.action.async.AsyncBuilder;
import io.simplesource.saga.action.async.AsyncResult;
import io.simplesource.saga.action.async.AsyncSpec;
import io.simplesource.saga.shared.topics.TopicConfigBuilder;

import java.util.concurrent.ScheduledExecutorService;

/**
 * The Http builder is a thin wrapper around the {@link AsyncBuilder} that makes it easier
 * to wrap Http web service calls as async actions.
 *
 * The client still has to provide an {@link HttpSpec#asyncHttpFunction}
 * implementation using the Http client of their choice.
 */
// TODO: does this belong in userland?
public final class HttpBuilder {

    /**
     * A static function that delegates the {@link AsyncBuilder} to provide an implementation of an
     * action processor that can process http web service calls asynchronously as part of a saga.
     *
     * @param <A> common representation type for all action commands (typically Json / GenericRecord for Avro)
     * @param <K> key for the output topic for the result of Http topics
     * @param <B> body for Http request
     * @param <O> value returned by the Http request - also normally quite generic
     * @param <R> final result type that ends up in value topic
     * @param httpSpec     the http spec
     * @param topicBuildFn the topic build fn
     * @param executor     the executor service that is used to invoke asynchronously and schedule timeouts
     * @return the action processor build step
     */
    public static <A, K, B, O, R> ActionProcessorBuildStep<A> apply(
            HttpSpec<A, K, B, O, R> httpSpec,
            TopicConfigBuilder.BuildSteps topicBuildFn,
            ScheduledExecutorService executor) {

        AsyncSpec<A, HttpRequest<K, B>, K, O, R> asyncSpec =
                AsyncSpec.of(
                        httpSpec.actionType,
                        httpSpec.decoder::decode,
                        httpSpec.asyncHttpFunction,
                        httpSpec.groupId,
                        httpSpec.outputSpec.map(o ->
                                AsyncResult.of(
                                        o.decoder::decode,
                                        r -> r.key,
                                        o.outputSerdes
                                )),
                        (request, o) -> httpSpec.undoFunction.apply(request, o),
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


