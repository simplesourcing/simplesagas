package io.simplesource.saga.client.api;

import io.simplesource.kafka.dsl.KafkaConfig;
import io.simplesource.kafka.internal.util.NamedThreadFactory;
import io.simplesource.saga.model.api.SagaAPI;
import io.simplesource.saga.model.serdes.SagaClientSerdes;
import io.simplesource.saga.model.specs.SagaClientSpec;
import io.simplesource.saga.shared.properties.PropertiesBuilder;
import io.simplesource.saga.shared.topics.TopicConfig;
import io.simplesource.saga.shared.topics.TopicConfigBuilder;
import io.simplesource.saga.shared.topics.TopicTypes;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static java.util.Objects.requireNonNull;

/**
 * The saga client builder is used to create a {@link SagaAPI}.
 * <p>
 * To create an instance of a {@link SagaAPI}, use code like this:
 * <pre>{@code
 * SagaAPI<A> sagaApi = SagaClientBuilder
 *     .create(a -> a.withBootstrapServers("kafka_broker:9092"))
 *     .withSerdes(serdes)
 *     .withClientId(clientId)
 *     .withScheduler(scheduler)
 *     .build();
 * }</pre>
 * @param <A> a representation of an action command that is shared across all actions in the saga. This is typically a generic type, such as Json, or if using Avro serialization, SpecificRecord or GenericRecord
 */
public final class SagaClientBuilder<A> {

    private PropertiesBuilder.BuildSteps propertiesBuildSteps = null;
    private ScheduledExecutorService scheduler = null;
    private SagaClientSerdes<A> serdes = null;
    private TopicConfigBuilder.BuildSteps topicConfigBuildSteps = builder -> builder;
    private String clientId = null;

    /**
     * Create saga client builder.
     *
     * @param <A> a representation of an action command that is shared across all actions in the saga. This is typically a generic type, such as Json, or if using Avro serialization, SpecificRecord or GenericRecord
     * @return the saga client builder
     */
    public static <A> SagaClientBuilder<A> create() {
        return new SagaClientBuilder<>();
    }

    /**
     * Create saga client builder with config properties.
     *
     * @param <A> a representation of an action command that is shared across all actions in the saga. This is typically a generic type, such as Json, or if using Avro serialization, SpecificRecord or GenericRecord
     * @param configBuildSteps a function that allows setting Kafka config properties incrementally.
     * @return the saga client builder
     */
    public static <A> SagaClientBuilder<A> create(PropertiesBuilder.BuildSteps configBuildSteps) {
        return SagaClientBuilder.<A>create().withProperties(configBuildSteps);
    }

    /**
     * Sets the config properties.
     *
     * @param configBuildSteps a sequence of configuration steps
     * @return the saga client builder
     */
    public SagaClientBuilder<A> withProperties(PropertiesBuilder.BuildSteps configBuildSteps) {
        this.propertiesBuildSteps = configBuildSteps;
        return this;
    }

    /**
     * Sets the Serdes required for the saga request and saga response topics.
     *
     * @param serdes the serdes for the action
     * @return the saga client builder
     */
    public SagaClientBuilder<A> withSerdes(SagaClientSerdes<A> serdes) {
        this.serdes = serdes;
        return this;
    }

    /**
     * Sets the scheduler required to schedule timeouts.
     *
     * @param scheduler the scheduler
     * @return the saga client builder
     */
    public SagaClientBuilder<A> withScheduler(ScheduledExecutorService scheduler) {
        this.scheduler = scheduler;
        return this;
    }

    /**
     * Sets the topic configuration for naming and creating the saga request and response topics.
     *
     * @param topicBuildFn a function to set topic configuration details incrementally.
     * @return the saga client builder
     */
    public SagaClientBuilder<A> withTopicConfig(TopicConfigBuilder.BuildSteps topicBuildFn) {

        return this;
    }

    /**
     * With client id saga client builder.
     *
     * @param clientId the client id
     * @return the saga client builder
     */
    public SagaClientBuilder<A> withClientId(String clientId) {
        this.clientId = clientId;
        return this;
    }

    /**
     * Builds the saga API.
     *
     * @return the saga API
     */
    public SagaAPI<A> build() {
        requireNonNull(propertiesBuildSteps, "Kafka properties have not been defined");
        requireNonNull(serdes, "Serdes have not been defined");
        requireNonNull(clientId, "ClientId has not been defined");

        if (scheduler == null)
            scheduler = Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("SagaApp-scheduler"));

        TopicConfig topicConfig = topicConfigBuildSteps
                .withInitialStep(tcBuilder -> tcBuilder.withTopicBaseName(TopicTypes.SagaTopic.SAGA_BASE_NAME))
                .build(TopicTypes.SagaTopic.client);

        SagaClientSpec<A> sagaSpec = SagaClientSpec.of(serdes);

        Properties properties = propertiesBuildSteps.build(PropertiesBuilder.Target.ApiClient);
        Map<String, Object> propsMap = new HashMap<>();
        properties.forEach((key, value) -> propsMap.put(key.toString(), value.toString()));

        KafkaConfig kafkaConfig = new KafkaConfig.Builder().withSettings(propsMap).build();
        return KafkaSagaAPI.of(sagaSpec, kafkaConfig, topicConfig, clientId, scheduler);
    }
}
