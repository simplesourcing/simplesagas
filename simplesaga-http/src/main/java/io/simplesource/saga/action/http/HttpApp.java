package io.simplesource.saga.action.http;


import io.simplesource.saga.action.async.AsyncApp;
import io.simplesource.saga.action.async.AsyncOutput;
import io.simplesource.saga.action.async.AsyncSpec;
import io.simplesource.saga.model.serdes.ActionSerdes;
import io.simplesource.saga.shared.topics.TopicConfigBuilder;
import io.simplesource.saga.shared.utils.StreamAppConfig;

import java.util.function.Supplier;

// TODO: does this belong in userland?
public final class HttpApp<A> {
    private final AsyncApp<A> asyncApp;

    public HttpApp(ActionSerdes<A> actionSerdes, TopicConfigBuilder.BuildSteps topicBuildFn) {
        asyncApp = new AsyncApp<>(actionSerdes, topicBuildFn);
    }

    public <K, B, O, R> HttpApp<A> addHttpProcessor(HttpSpec<A, K, B, O, R> httpSpec) {
        AsyncSpec<A, HttpRequest<K, B>, K, O, R> asyncSpec = new AsyncSpec<>(
                httpSpec.actionType,
                httpSpec.decoder::decode,
                httpSpec.asyncHttpClient,
                httpSpec.groupId,
                httpSpec.outputSpec.map(o ->
                        new AsyncOutput<>(o.decoder::decode, o.serdes,
                                r -> r.key,
                                r -> r.topicName,
                                o.topicCreations)),
                httpSpec.timeout);
        asyncApp.addAsync(asyncSpec);
        return this;
    }

    public <D, K, O, R> HttpApp<A> addAsync(AsyncSpec<A, D, K, O, R> spec) {
        asyncApp.addAsync(spec);
        return this;
    }


    public HttpApp<A> addCloseHandler(Supplier<Integer> handler) {
        asyncApp.addCloseHandler(handler);
        return this;
    }

    public void run(StreamAppConfig appConfig) {
        asyncApp.run(appConfig);
    }
}



