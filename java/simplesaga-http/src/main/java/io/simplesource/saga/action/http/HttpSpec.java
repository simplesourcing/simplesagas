package io.simplesource.saga.action.http;

import io.simplesource.saga.action.async.Callback;
import lombok.Value;

import java.time.Duration;
import java.util.Optional;
import java.util.function.BiConsumer;

/**
  * @param <A> - common representation type for all action commands (typically Json / GenericRecord for Avro)
  * @param <K> - key for the output topic
  * @param <B> - body for Http request
  * @param <O> - output returned by the Http request - also normally quite generic
  * @param <R> - final result type that ends up in output topic
  */
@Value
public final class HttpSpec<A, K, B, O, R> {
    public final String actionType;
    public final HttpRequest.HttpRequestDecoder<A, K, B> decoder;
    public final BiConsumer<HttpRequest<K, B>, Callback<O>> asyncHttpClient;
    public final String groupId;
    public final Optional<HttpOutput<K, O, R>> outputSpec;
    public final Optional<Duration> timeout;
}
