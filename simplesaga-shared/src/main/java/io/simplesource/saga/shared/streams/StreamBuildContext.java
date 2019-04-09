package io.simplesource.saga.shared.streams;

import io.simplesource.saga.shared.kafka.PropertiesBuilder;
import lombok.Value;

@Value(staticConstructor = "of")
public class StreamBuildContext<I> {
    public final I appInput;
    public final PropertiesBuilder.BuildSteps properties;
}
