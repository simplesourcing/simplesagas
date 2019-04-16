package io.simplesource.saga.model.saga;

import java.time.Duration;
import java.util.Optional;

/**
 * The interface Retry strategy.
 */
@FunctionalInterface
public interface RetryStrategy {
    /**
     * Next retry optional.
     *
     * @param retryCount the retry count
     * @return the optional
     */
    Optional<Duration> nextRetry(int retryCount);

    /**
     * Fail fast retry strategy.
     *
     * @return the retry strategy
     */
    static RetryStrategy failFast() {
        return i -> Optional.empty();
    }

    /**
     * Repeat retry strategy.
     *
     * @param numberOfRetries the number of retries
     * @param delay           the delay
     * @return the retry strategy
     */
    static RetryStrategy repeat(int numberOfRetries, Duration delay) {
        return i -> (i < numberOfRetries) ? Optional.of(delay) : Optional.empty();
    }
}
