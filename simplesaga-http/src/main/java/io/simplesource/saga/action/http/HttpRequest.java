package io.simplesource.saga.action.http;

import io.simplesource.data.Result;
import lombok.Value;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;

/**
  * @param <K> - key for the value topic
  * @param <B> - body for Http request
  */
@Value
public final class HttpRequest<K, B> {
    public interface HttpRequestDecoder<A, K, B> {
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

    public static <K> HttpRequest<K, ?> of(K key, HttpVerb verb, String url) {
        return new HttpRequest<>(key, verb, url, Collections.emptyMap(), Optional.empty(), Optional.empty());
    }

    public static <K> HttpRequest<K, ?> of(K key, HttpVerb verb, String url, String topicName) {
        return new HttpRequest<>(key, verb, url, Collections.emptyMap(), Optional.empty(), Optional.ofNullable(topicName));
    }

    public static <K, B> HttpRequest<K, B> ofWithBody(K key, HttpVerb verb, String url, String topicName, B body) {
        return new HttpRequest<>(key, verb, url, Collections.emptyMap(), Optional.ofNullable(body), Optional.ofNullable(topicName));
    }
}
