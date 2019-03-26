package io.simplesource.saga.action.http;

import io.simplesource.saga.action.async.AsyncBuilder;
import io.simplesource.saga.action.async.AsyncOutput;
import io.simplesource.saga.action.async.AsyncSpec;
import io.simplesource.saga.model.specs.ActionProcessorSpec;
import io.simplesource.saga.shared.streams.StreamBuildStep;
import io.simplesource.saga.shared.topics.TopicConfigBuilder;

import java.util.concurrent.ScheduledExecutorService;

// TODO: does this belong in userland?
public final class HttpBuilder {

    public static <A, K, B, O, R> StreamBuildStep<ActionProcessorSpec<A>> apply(
            HttpSpec<A, K, B, O, R> httpSpec,
            TopicConfigBuilder.BuildSteps topicBuildFn,
            ScheduledExecutorService executor) {

        AsyncSpec<A, HttpRequest<K, B>, K, O, R> asyncSpec =
                new AsyncSpec<>(
                httpSpec.actionType,
                httpSpec.decoder::decode,
                httpSpec.asyncHttpClient,
                httpSpec.groupId,
                httpSpec.outputSpec.map(o ->
                        new AsyncOutput<>(o.decoder::decode, o.outputSerdes,
                                r -> r.key,
                                r -> r.topicName,
                                o.topicCreations)),
                httpSpec.timeout);

        return AsyncBuilder.apply(asyncSpec, topicBuildFn, executor);
    }
}


