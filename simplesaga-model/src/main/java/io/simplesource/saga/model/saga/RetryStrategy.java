package io.simplesource.saga.model.saga;

import java.time.Duration;
import java.util.Optional;

/**
 * This interface specifies how the saga coordinator should attempt retries, both how many times and how long between them
 */
@FunctionalInterface
public interface RetryStrategy {
    /**
     * A wait period until the next retry. {@code Optional.empty()} indicates no retry should be processed, and action should be failed.
     *
     * @param retryCount the number of retries that have already been been processed.
     * @return the optional wait period until the next retry.
     */
    Optional<Duration> nextRetry(int retryCount);

    /**
     * This strategy indicates no retries should be processed.
     *
     * @return the retry strategy
     */
    static RetryStrategy failFast() {
        return i -> Optional.empty();
    }

    /**
     * This strategy retries a certain number of times, with a fixed delay between attempts.
     *
     * @param numberOfRetries the number of retries
     * @param delay           the delay
     * @return the retry strategy
     */
    static RetryStrategy repeat(int numberOfRetries, Duration delay) {
        return i -> (i < numberOfRetries) ? Optional.of(delay) : Optional.empty();
    }
}
