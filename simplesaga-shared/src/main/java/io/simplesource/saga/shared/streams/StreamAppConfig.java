package io.simplesource.saga.shared.streams;

import lombok.Value;

@Value(staticConstructor = "of")
public class StreamAppConfig {
    public final String appId;
    public final String bootstrapServers;
}



