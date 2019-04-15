package io.simplesource.saga.action.async;

import io.simplesource.data.Result;
import io.simplesource.saga.model.action.UndoCommand;
import lombok.*;

import java.time.Duration;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.Function;

/**
 * A value class with the information required to define an async action processor.
 * <p>
 * This includes:
 * <ul>
 *     <li>The conversion from the shared type {@code A} to the decode input type {@code D}.</li>
 *     <li>The asynchronous function itself.</li>
 *     <li>An undo (compensation) action if there is one.</li>
 *     <li>The conversion to an output result type, if serialised output is required</li>
 *     <li>The serdes for serialisation to the output topic</li>
 * </ul>
 *
 *
 * @param <A> - common representation form for all action commands (typically Json / GenericRecord for Avro)
 * @param <D> - intermediate decoded input type (this can be specific to this action processor)
 * @param <K> - key for the value topic
 * @param <O> - value returned by async function
 * @param <R> - final result type that ends up in value topic
 */
@Value
@Builder
@AllArgsConstructor(staticName = "of")
public final class AsyncSpec<A, D, K, O, R> {
    /**
     * The UndoFunction functional interface.
     */
    @FunctionalInterface
    public interface UndoFunction<A, D, O> {
        /**
         * The UndoFunction is a mechanism to take the result of the async invocation and turning it into an
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
        Optional<UndoCommand<A>> apply(D decodedInput, O output);
    }

    /**
     * Each action processor must have a unique action type.
     */
    public final String actionType;
    /**
     * The Input decoder. This converts the common action command representation to an intermediate decoded type.
     * The {@code A} type is common across all action processors and action types. This enables working with a type that
     * is specific to this action processor.
     */
    public final Function<A, Result<Throwable, D>> inputDecoder;
    /**
     * This is a {@link BiConsumer} that takes the decoded input type and a {@link Callback} as input.
     * It then executes and, completes the call back when it is finished.
     */
    public final BiConsumer<D, Callback<O>> asyncFunction;
    /**
     * This group Id is used to create a consumer group that consumes requests to invoke asynchronous actions.
     * This consumer group ensures that the load of submitting async actions actions is distributed across instances.
     */
    public final String groupId;
    /**
     * This is required for cases where the result is written to an output topic, or where an undo function is
     * defined based on the return value.
     */
    public final Optional<AsyncResult<D, K, O, R>> resultSpec;
    /**
     * An undo function.
     */
    public final UndoFunction<A, D, O> undoFunction;
    /**
     * The timeout.
     */
    public final Optional<Duration> timeout;
}
