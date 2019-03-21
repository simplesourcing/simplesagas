package io.simplesource.saga.action.async;

import io.simplesource.data.Result;
import io.simplesource.saga.shared.topics.TopicCreation;
import lombok.Value;

import java.util.List;
import java.util.Optional;
import java.util.function.Function;

@Value
public final class AsyncOutput<D, K, O, R> {
    public final Function<O, Optional<Result<Throwable, R>>> outputDecoder;
    public final AsyncSerdes<K, R>   serdes;
    public final Function<D, K> keyMapper;
    public final Function<D, Optional<String>> topicName;
    public final List<TopicCreation> topicCreations;
}
