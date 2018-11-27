package io.simplesource.saga.model.specs;

import io.simplesource.saga.model.serdes.ActionSerdes;
import lombok.Value;

@Value
public class ActionProcessorSpec<A> {
    public final ActionSerdes<A> serdes;
}
