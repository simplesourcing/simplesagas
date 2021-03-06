package io.simplesource.saga.action.async;

import io.simplesource.data.Result;

import java.util.function.Consumer;

/**
 * A callback function that is called by the asynchronous function to indicate that processing is complete,
 * and to pass the result back to the async action processor.
 *
 * @param <O> the output type generated by the async function
 */
@FunctionalInterface
public interface Callback<O> {
    /**
     * A function that is called during the async invocation to pass the return value back to the action processor.
     *
     * @param result the result - either a {@link Throwable} if there is an error, or the return value O
     */
    void complete(Result<Throwable, O> result);
}
