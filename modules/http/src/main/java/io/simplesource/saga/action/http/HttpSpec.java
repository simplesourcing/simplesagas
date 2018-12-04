package io.simplesource.saga.action.http;

import io.simplesource.data.Result;
import io.simplesource.saga.action.async.AsyncSerdes;
import io.simplesource.saga.action.async.CallBack;
import io.simplesource.saga.shared.topics.TopicCreation;
import lombok.Value;

import java.util.List;
import java.util.Optional;
import java.util.function.BiConsumer;

/**
  * @param <K> - key for the output topic
  * @param <O> - output returned by the Http request - also normally quite generic
  * @param <R> - final result type that ends up in output topic
  */
@Value
final class HttpOutput<K, O, R> {
    interface HttpResultDecoder<O, R> {
        Optional<Result<Throwable, R>> decode(O output);
    }

    public final HttpResultDecoder<O, R> decoder;
    public final AsyncSerdes<K, R> serdes;
    public final List<TopicCreation> topicCreations;
}

/**
  * @param <A> - common representation type for all action commands (typically Json / GenericRecord for Avro)
  * @param <K> - key for the output topic
  * @param <B> - body for Http request
  * @param <O> - output returned by the Http request - also normally quite generic
  * @param <R> - final result type that ends up in output topic
  */
@Value
final class HttpSpec<A, K, B, O, R> {
    public final String actionType;
    public final HttpRequest.HttpRequestDecoder<A, K, B> decoder;
    public final BiConsumer<HttpRequest<K, B>, CallBack<O>> asyncHttpClient;
    public final String groupId;
    public final Optional<HttpOutput<K, O, R>> outputSpec;
}
