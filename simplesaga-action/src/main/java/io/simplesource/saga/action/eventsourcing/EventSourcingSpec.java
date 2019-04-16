package io.simplesource.saga.action.eventsourcing;

import io.simplesource.data.Result;
import io.simplesource.data.Sequence;
import io.simplesource.kafka.api.CommandSerdes;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Value;

import java.time.Duration;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * A value class with the information required to define an event sourcing action processor.
 *
 * @param <A> common representation form for all action commands (typically Json / GenericRecord for Avro)
 * @param <D> intermediate decoded input type (that can easily be converted to both K and C)
 * @param <K> aggregate key type
 * @param <C> simple sourcing command type
 *
 * @see io.simplesource.kafka.spec.CommandSpec
 */
@Value
@Builder
@AllArgsConstructor(staticName = "of")
public final class EventSourcingSpec<A, D, K, C> {
    /**
     * The UndoFunction functional interface.
     */
    @FunctionalInterface
    public interface UndoFunction<A, K, C> {
        /**
         * The UndoFunction is a mechanism to take the result of the async invocation and turning it into an
         * undo action command. This action command is passed back to the saga coordinator, and if the saga fails,
         * this undo function will be invoked.
         * <p>
         * The return value of type A must satisfy the following property:
         * <p>
         * Whatever value {@code command} tranforms the aggregate to, if one takes the result of the {@code UndoFunction.apply},
         * decode it, and then apply it to this new aggregate value, it should result in the original aggregate value being restored.
         * <p>
         * Note that the undo command of an event sourcing action processor must have the same action type.
         *
         * @param aggregateKey  the aggregate key
         * @param command       the Simple Sourcing command
         * @return an undo command as an {@code A}, or {@code Optional.empty()} if no undo function is to be generated
         */
        Optional<A> apply(K aggregateKey, C command);
    }

    /**
     * The Action type. Each action processor must have a unique action type.
     */
    public final String actionType;
    /**
     * The Simple Sourcing aggregate name.
     */
    public final String aggregateName;
    /**
     * The Input decoder. This converts the common action command representation to an intermediate decoded type.
     * The {@code A} type is common across all action processors and action types. This enables working with a type that
     * is specific to this action processor.
     */
    public final Function<A, Result<Throwable, D>> inputDecoder;
    /**
     * The Command mapper maps the decoded input type to a Simple Sourcing command.
     */
    public final Function<D, C> commandMapper;
    /**
     * The Key mapper maps the decoded input type to the aggregate key.
     */
    public final Function<D, K> keyMapper;
    /**
     * The Sequence mapper maps the decoded input type to the last read aggregate sequence.
     * <p>
     * With Simple Sourcing, updates to aggregates are versioned with a sequence number.
     * With sequence checking enabled, the client specifies the last read sequence number.
     * When Simple Sourcing processes the command request, it verifies that the sequence number
     * of the aggregate is the same as the one specified by the client. This ensures that the command is being applied to
     * the latest version of the aggregate.
     * <p>
     * With sagas, there may be more than one operation on a given aggregate instance. The client must specify the correct
     * sequence number for the action that is executed first. After that the saga coordinator keeps track of the sequence number of
     * the most recent write to the aggregate instance within the saga.
     * This enables an optimistic lock to be held for the duration of the saga.
     */
    public final Function<D, Sequence> sequenceMapper;
    /**
     * The Undo command.
     * Note that in contrast to {@link io.simplesource.saga.action.async.AsyncContext async action undos}
     * an undo command for a Simple Sourcing action must be of the same action type .
     */
    public final UndoFunction<A, K, C> undoCommand;
    /**
     * The Command serdes are required for the command request and response topics.
     */
    public final CommandSerdes<K, C> commandSerdes;
    /**
     * The Timeout. Currently not fully implemented - used to determine the maximum length of the join windows for waiting for
     * responses from the command response topic
     */
    public final Duration timeout;
}
