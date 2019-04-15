package io.simplesource.saga.model.specs;

import io.simplesource.kafka.spec.WindowSpec;
import io.simplesource.saga.model.serdes.SagaSerdes;
import lombok.Value;

/**
 * The type Saga spec.
 *
 * @param <A> the type parameter
 */
@Value
public class SagaSpec<A> {
    /**
     * The Serdes.
     */
    public final SagaSerdes<A> serdes;
    /**
     * The Response window.
     */
    public final WindowSpec responseWindow;
}

