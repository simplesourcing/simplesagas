package io.simplesource.saga.shared.kafka;

import io.simplesource.saga.shared.streams.StreamAppConfig;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;

import java.util.*;

/***
 *
 */
public class PropertiesBuilder {
    private final Properties properties = new Properties();

    public static PropertiesBuilder create() {
        return new PropertiesBuilder();
    }

    public PropertiesBuilder withProperty(String key, Object value) {
        this.properties.put(key, value);
        return this;
    }

    public PropertiesBuilder withProperties(Properties properties) {
        properties.forEach(this.properties::put);
        return this;
    }

    public PropertiesBuilder withProperties(Map<String, Object> properties) {
        properties.forEach(this.properties::put);
        return this;
    }

    public PropertiesBuilder withStreamAppConfig(StreamAppConfig config) {
        return this
                .withProperty(StreamsConfig.APPLICATION_ID_CONFIG, config.appId)
                .withProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, config.bootstrapServers);
    }

    public PropertiesBuilder withDefaultConsumerProps() {
        return this
                .withProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true)
                .withProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 1000)
                .withProperty(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed")
                .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    }

    public PropertiesBuilder withDefaultProducerProps() {
        return this
                .withProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true)
                .withProperty(ProducerConfig.RETRIES_CONFIG, 3)
                .withProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy")
                .withProperty(ProducerConfig.ACKS_CONFIG, "all");
    }

    public PropertiesBuilder withDefaultStreamProps() {
        return this
                .withProperty(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE)
                .withProperty(StreamsConfig.STATE_DIR_CONFIG, "/tmp/kafka-streams")
                .withProperty(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, LogAndContinueExceptionHandler.class)
                .withProperty(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE)
                .withProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy")
                .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    }

    public Properties build() {
        return properties;
    }

    @FunctionalInterface
    public interface BuildSteps {
        PropertiesBuilder applyStep(PropertiesBuilder builder);

        default BuildSteps withNextStep(BuildSteps initial) {
            return builder -> initial.applyStep(this.applyStep(builder));
        }

        default BuildSteps withInitialStep(BuildSteps initial) {
            return builder -> this.applyStep(initial.applyStep(builder));
        }

        default Properties build() {
            return this
                    .applyStep(PropertiesBuilder.create()).build();
        }
    }
}
