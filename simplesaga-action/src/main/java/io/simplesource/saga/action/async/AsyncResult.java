package io.simplesource.saga.action.async;

import io.simplesource.data.Result;
import io.simplesource.saga.model.serdes.TopicSerdes;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Value;

import java.util.Optional;
import java.util.function.Function;

/**
 * Represents the information required to consume the output
 * returned by an async action, convert it to the result type {@code R}, and
 * save it to an output topic.
 *
 * @param <D> intermediate decoded input type
 * @param <K> key for the output topic (if the result of async invocation is written to an output topic)
 * @param <O> output value returned by async function
 * @param <R> final result type that ends up in output topic
 */
@Value(staticConstructor = "of")
@Builder
@AllArgsConstructor(staticName = "of")
public final class AsyncResult<D, K, O, R> {

    /**
     * A function that takes the output of the async invocation and decodes it / converts it to the required result type.
     * <p>
     * For example, the async function may be a call to a web service that returns a Json payload.
     * This function is then be used to convert the Json to the desired result type {@code R}.
     */
    public final Function<O, Optional<Result<Throwable, R>>> outputMapper;
    /**
     * The Key mapper converts the decoded input type to a key for the output topic.
     */
    public final Function<D, K> keyMapper;
    /**
     * The serdes for writing the result to the output topic.
     */
    public final TopicSerdes<K, R> outputSerdes;
}
