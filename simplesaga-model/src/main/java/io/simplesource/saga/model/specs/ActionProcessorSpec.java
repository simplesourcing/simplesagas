package io.simplesource.saga.model.specs;

import io.simplesource.saga.model.serdes.ActionSerdes;
import lombok.Value;

@Value(staticConstructor = "of")
public class ActionProcessorSpec<A> {
    public final ActionSerdes<A> serdes;
}
