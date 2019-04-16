package io.simplesource.saga.model.saga;

import io.simplesource.data.Error;
import lombok.Value;

import java.util.function.BiFunction;

/**
 * Represents an error in saga processing
 */
@Value
public class SagaError {
    /**
     * Construct a {@link SagaError} from a {@link Throwable}.
     *
     * @param reason    the reason value
     * @param throwable for more context
     * @return the constructed {@link SagaError}
     */
    public static SagaError of(final Reason reason, final Throwable throwable) {
        return new SagaError(Error.of(reason, throwable));
    }

    /**
     * Construct a {@link SagaError} from a {@link Throwable}.
     *
     * @param reason the reason value
     * @param msg    for more context
     * @return the constructed {@link SagaError}
     */
    public static SagaError of(final Reason reason, final String msg) {
        return new SagaError(Error.of(reason, msg));
    }

    /**
     * The reason value accessor.
     *
     * @return the reason
     */
    public Reason getReason() {
        return error.getReason();
    }

    /**
     * The error message accessor
     *
     * @return the error message
     */
    public String getMessage() {
        return error.getMessage();
    }

    private final Error<Reason> error;

    private SagaError(final Error<Reason> error) {
        this.error = error;
    }

    /**
     * Represents the reason for an action processor or saga failing.
     */
// TODO: flesh out error cases
    public enum Reason {
        /**
         * The build steps used to create a saga are invalid.
         */
        InvalidSaga,
        /**
         * A saga or action processing step timed out.
         */
        Timeout,
        /**
         * There was an error with a simple sourcing or other command.
         */
        CommandError,
        /**
         * An undetermined error occurred.
         */
        InternalError,
        /**
         * An unexpected error occurred.
         */
        UnexpectedErrorCode
    }
}
