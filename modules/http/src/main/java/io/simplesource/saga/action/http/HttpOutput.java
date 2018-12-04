package io.simplesource.saga.action.http;

import io.simplesource.data.Result;
import io.simplesource.saga.action.async.AsyncSerdes;
import io.simplesource.saga.shared.topics.TopicCreation;
import lombok.Value;

import java.util.List;
import java.util.Optional;

/**
  * @param <K> - key for the output topic
  * @param <O> - output returned by the Http request - also normally quite generic
  * @param <R> - final result type that ends up in output topic
  */
@Value
public final class HttpOutput<K, O, R> {
    interface HttpResultDecoder<O, R> {
        Optional<Result<Throwable, R>> decode(O output);
    }

    public final HttpResultDecoder<O, R> decoder;
    public final AsyncSerdes<K, R> serdes;
    public final List<TopicCreation> topicCreations;
}
