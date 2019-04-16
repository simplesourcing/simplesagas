package io.simplesource.saga.model.config;

import lombok.Value;

/**
 * The essential configuration required by all KStream apps.
 */
@Value(staticConstructor = "of")
public class StreamAppConfig {
    /**
     * The application id, used for consumer groups and internal topic prefixes.
     */
    public final String appId;
    /**
     * The bootstrap servers, used to connect to the Kafka cluster.
     */
    public final String bootstrapServers;
}



