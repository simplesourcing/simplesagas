package io.simplesource.saga.action.app;

import io.simplesource.saga.model.specs.ActionSpec;
import io.simplesource.saga.shared.kafka.PropertiesBuilder;
import lombok.Value;

import java.util.Properties;

@Value(staticConstructor = "of")
public class ActionAppContext<A> {
    public final ActionSpec<A> actionSpec;
    public final PropertiesBuilder.BuildSteps properties;
}
