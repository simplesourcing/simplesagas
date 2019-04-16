package io.simplesource.saga.model.config;

import lombok.Value;

@Value(staticConstructor = "of")
public class StreamAppConfig {
    public final String appId;
    public final String bootstrapServers;
}



