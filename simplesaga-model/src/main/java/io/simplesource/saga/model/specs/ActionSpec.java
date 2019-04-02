package io.simplesource.saga.model.specs;

import io.simplesource.saga.model.serdes.ActionSerdes;
import lombok.Value;

import java.time.Duration;

@Value(staticConstructor = "of")
public class ActionSpec<A> {
    public final ActionSerdes<A> serdes;
}
