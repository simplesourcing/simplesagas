package io.simplesource.saga.action.http;

import io.simplesource.data.Result;
import io.simplesource.saga.model.serdes.TopicSerdes;
import io.simplesource.saga.shared.topics.TopicCreation;
import lombok.Value;

import java.util.List;
import java.util.Optional;

/**
  * @param <K> - key for the value topic
  * @param <O> - value returned by the Http request - also normally quite generic
  * @param <R> - final result type that ends up in value topic
  */
@Value
public final class HttpOutput<K, O, R> {
    public interface HttpResultDecoder<O, R> {
        Optional<Result<Throwable, R>> decode(O output);
    }

    public final HttpResultDecoder<O, R> decoder;
    public final TopicSerdes<K, R> outputSerdes;
    public final List<TopicCreation> topicCreations;
}
