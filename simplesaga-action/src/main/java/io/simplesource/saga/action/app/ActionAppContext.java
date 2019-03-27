package io.simplesource.saga.action.app;

import io.simplesource.saga.model.specs.ActionSpec;
import lombok.Value;

import java.util.Properties;

@Value(staticConstructor = "of")
public class ActionAppContext<A> {
    public final ActionSpec<A> actionSpec;
    public final Properties properties;
}
