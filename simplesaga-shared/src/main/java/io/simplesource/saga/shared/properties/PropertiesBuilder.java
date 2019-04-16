package io.simplesource.saga.shared.properties;

import io.simplesource.saga.model.config.StreamAppConfig;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;

import java.util.*;

/***
 * PropertiesBuilder, as used with {@link BuildSteps PropertiesBuilder.BuildSteps} is a functional pattern for setting properties
 *
 * Whenever a {@link BuildSteps PropertiesBuilder.BuildSteps} is required, a user can provide a lambda function of this form for example:
 * <pre>{@code
 * builder -> builder
 *     .withProperty("property1", value1)
 *     .withProperty("property2", value2)
 * }*</pre>
 *
 * One of the key advantages of this pattern is it allows the framework flexibility how it treats these parameters, without complicating the user interaction.
 * <p>
 * For example, the framework may provide a default for "property1", which the user can override,
 * and it may take the user's value for "property2" and apply some post processing to it.
 */
public class PropertiesBuilder {
    private final Properties properties = new Properties();

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

    public PropertiesBuilder withBootstrapServers(String bootstrapServers) {
        return this
                .withProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    }

    /**
     * Sets the essential properties for Kafka Streams applications (app id and bootstrap servers)
     *
     * @param config the config
     * @return the properties builder
     */
    public PropertiesBuilder withStreamAppConfig(StreamAppConfig config) {
        return this
                .withProperty(StreamsConfig.APPLICATION_ID_CONFIG, config.appId)
                .withProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, config.bootstrapServers);
    }

    /**
     * Sets default properties for Kafka consumers
     *
     * @return the properties builder
     */
    public PropertiesBuilder withDefaultConsumerProps() {
        return this
                .withProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true)
                .withProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 1000)
                .withProperty(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed")
                .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    }

    /**
     * Sets default properties for Kafka producers
     *
     * @return the properties builder
     */
    public PropertiesBuilder withDefaultProducerProps() {
        return this
                .withProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true)
                .withProperty(ProducerConfig.RETRIES_CONFIG, 3)
                .withProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy")
                .withProperty(ProducerConfig.ACKS_CONFIG, "all");
    }

    /**
     * Sets default properties for Kafka Streams applications
     *
     * @return the properties builder
     */
    public PropertiesBuilder withDefaultStreamProps() {
        return this
                .withProperty(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE)
                .withProperty(StreamsConfig.STATE_DIR_CONFIG, "/tmp/kafka-streams")
                .withProperty(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, LogAndContinueExceptionHandler.class)
                .withProperty(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE)
                .withProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy")
                .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    }

    /**
     * Gets a property value.
     *
     * @return the properties
     */
    Object get(String key) {
        return properties.get(key);
    }


    /**
     * A functional interface representing a configuration step that is applied to a properties builder
     */
    @FunctionalInterface
    public interface BuildSteps {

        PropertiesBuilder applyStep(PropertiesBuilder builder);

        default BuildSteps withInitialStep(BuildSteps initial) {
            return builder -> this.applyStep(initial.applyStep(builder));
        }

        default BuildSteps withNextStep(BuildSteps initial) {
            return builder -> initial.applyStep(this.applyStep(builder));
        }

        default Properties build() {
            return this
                    .applyStep(new PropertiesBuilder()).properties;
        }
    }
}
