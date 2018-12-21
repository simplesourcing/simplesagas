package io.simplesource.saga.action.async;

import io.simplesource.data.Result;
import io.simplesource.saga.shared.topics.TopicCreation;
import lombok.Value;

import java.util.List;
import java.util.Optional;
import java.util.function.Function;

@Value
public final class AsyncOutput<I, K, O, R> {
    Function<O, Optional<Result<Throwable, R>>> outputDecoder;
    AsyncSerdes<K, R>   serdes;
    Function<I, Optional<String>> topicName;
    List<TopicCreation> topicCreations;
}
