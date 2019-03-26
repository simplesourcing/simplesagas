package io.simplesource.saga.shared.streams;

import lombok.Value;

import java.util.Properties;

@Value(staticConstructor = "of")
public class StreamBuildContext<I> {
    public final I appInput;
    public final Properties properties;
}
