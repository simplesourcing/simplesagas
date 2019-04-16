package io.simplesource.saga.shared.app;

import io.simplesource.saga.shared.properties.PropertiesBuilder;
import lombok.Value;

@Value(staticConstructor = "of")
public class StreamBuildContext<I> {
    public final I appInput;
    public final PropertiesBuilder.BuildSteps properties;
}
