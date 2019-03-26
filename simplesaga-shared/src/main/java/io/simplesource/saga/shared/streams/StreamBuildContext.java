package io.simplesource.saga.shared.streams;

import lombok.Value;

import java.util.Properties;

@Value
public class StreamBuildContext<I> {
    public final I buildInput;
    public final Properties config;
}
