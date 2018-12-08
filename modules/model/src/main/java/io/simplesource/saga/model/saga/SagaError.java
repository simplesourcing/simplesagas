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

    /**
     * Access the reason value and either the string or the throwable context and return whatever you like.
     *
     * @param <A> the result type
     * @param str the function that receives the reason and string, returning a value of the specified type
     * @param ex the function that receives the reason and throwable, returning a value of the specified type
     * @return the result
     */
    public <A> A fold(BiFunction<Reason, String, A> str, BiFunction<Reason, Throwable, A> ex){
        return error.fold(str, ex);
    }

    private final Error<Reason> error;

    private SagaError(final Error<Reason> error) {
        this.error = error;
    }

    public enum Reason {
        Timeout,
        CommandError,
        InternalError,
        UnexpectedErrorCode
    }
}
