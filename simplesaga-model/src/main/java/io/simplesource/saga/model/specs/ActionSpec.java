package io.simplesource.saga.model.specs;

import io.simplesource.saga.model.serdes.ActionSerdes;
import lombok.Value;

import java.time.Duration;

/**
 * Represents the details common to all action processors
 *
 * @param <A> a representation of an action command that is shared across all actions in the saga. This is typically a generic type, such as Json, or if using Avro serialization, SpecificRecord or GenericRecord
 */
@Value(staticConstructor = "of")
public class ActionSpec<A> {
    /**
     * The Serdes.
     */
    public final ActionSerdes<A> serdes;
}
