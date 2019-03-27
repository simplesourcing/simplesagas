package io.simplesource.saga.model.saga;

import io.simplesource.data.Error;
import lombok.Value;

import java.util.function.BiFunction;

@Value
public class SagaError {
    /**
     * Construct a {@link SagaError} from a {@link Throwable}.
     *
     * @param reason the reason value
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
     * @param msg for more context
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

    // TODO: flesh out error cases
    public enum Reason {
        InvalidSaga,
        Timeout,
        CommandError,
        InternalError,
        UnexpectedErrorCode
    }
}
