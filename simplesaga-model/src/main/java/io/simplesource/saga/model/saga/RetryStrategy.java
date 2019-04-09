package io.simplesource.saga.model.saga;

import java.time.Duration;
import java.util.Optional;

public interface RetryStrategy {
    Optional<Duration> nextRetry(int retryCount);

    static RetryStrategy failFast() {
        return i -> Optional.empty();
    }

    static RetryStrategy repeat(int numberOfRetries, Duration delay) {
        return i -> (i < numberOfRetries) ? Optional.of(delay) : Optional.empty();
    }
}
