package io.simplesource.saga.action.app;

import io.simplesource.saga.model.specs.ActionProcessorSpec;
import lombok.Value;

import java.util.Properties;

@Value(staticConstructor = "of")
public class ActionProcessorAppContext<A> {
    public final ActionProcessorSpec<A> actionProcessorSpec;
    public final Properties properties;
}
