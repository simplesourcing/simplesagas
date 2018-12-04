package io.simplesource.saga.action.http;

import io.simplesource.data.Result;
import lombok.Value;

import java.util.Map;
import java.util.Optional;

/**
  * @param <K> - key for the output topic
  * @param <B> - body for Http request
  */
@Value
public final class HttpRequest<K, B> {

    interface HttpRequestDecoder<A, K, B> {
        Result<Throwable,  HttpRequest<K, B>> decode(A input);
    }

    public enum HttpVerb {
        Get,
        Post,
        Put,
        Delete
    }

    public final K key;
    public final HttpVerb verb;
    public final String url;
    public final Map<String, String> headers;
    public final Optional<B> body;
    public final Optional<String> topicName;
}
