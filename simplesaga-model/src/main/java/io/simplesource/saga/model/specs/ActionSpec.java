package io.simplesource.saga.model.specs;

import io.simplesource.saga.model.serdes.ActionSerdes;
import lombok.Value;

import java.time.Duration;

/**
 * The type Action spec.
 *
 * @param <A> the type parameter
 */
@Value(staticConstructor = "of")
public class ActionSpec<A> {
    /**
     * The Serdes.
     */
    public final ActionSerdes<A> serdes;
}
