package io.simplesource.saga.model.specs;

import io.simplesource.kafka.spec.WindowSpec;
import io.simplesource.saga.model.serdes.SagaSerdes;
import lombok.Value;

@Value
public class SagaSpec<A> {
    public final SagaSerdes<A> serdes;
    public final WindowSpec responseWindow;
}

