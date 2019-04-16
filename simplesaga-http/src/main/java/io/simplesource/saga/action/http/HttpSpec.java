package io.simplesource.saga.action.http;

import io.simplesource.saga.action.async.Callback;
import io.simplesource.saga.model.action.UndoCommand;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Value;

import java.time.Duration;
import java.util.Optional;
import java.util.function.BiConsumer;

/**
  * @param <A> common representation type for all action commands (typically Json / GenericRecord for Avro)
  * @param <K> key for the output topic for the result of Http topics
  * @param <B> body for Http request
  * @param <O> value returned by the Http request - also normally quite generic
  * @param <R> final result type that ends up in value topic
  */
@Value
@Builder
@AllArgsConstructor(staticName = "of")
public final class HttpSpec<A, K, B, O, R> {
    /**
     * The UndoFunction functional interface.
     */
    @FunctionalInterface
    public interface UndoFunction<A, K, B, O> {
        /**
         * The UndoFunction is a mechanism to take the result of the async http invocation and turning it into an
         * undo action command. This action command is passed back to the saga coordinator, and if the saga fails,
         * this undo function will be invoked.
         * <p>
         * For example, if the async function calls an endpoint to book a hotel, the return value will include a booking reference.
         * This booking reference is then used to create an undo action command that calls the corresponding cancel endpoint,
         * passing in the booking reference
         * <p>
         * @param decodedInput the decoded input type
         * @param output       the result of the async invocation
         * @return an undo command, or {@code Optional.empty()} if no undo function is to be generated
         */
        Optional<UndoCommand<A>> apply(HttpRequest<K, B> decodedInput, O output);
    }

    public final String actionType;
    public final HttpRequest.HttpRequestDecoder<A, K, B> decoder;
    public final BiConsumer<HttpRequest<K, B>, Callback<O>> asyncHttpFunction;
    public final String groupId;
    public final Optional<HttpOutput<K, O, R>> outputSpec;
    public final UndoFunction<A, K, B, O> undoFunction;
    public final Optional<Duration> timeout;
}
