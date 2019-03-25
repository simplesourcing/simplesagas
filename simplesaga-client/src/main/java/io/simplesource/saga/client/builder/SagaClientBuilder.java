package io.simplesource.saga.client.builder;

import io.simplesource.kafka.dsl.KafkaConfig;
import io.simplesource.kafka.internal.util.NamedThreadFactory;
import io.simplesource.kafka.spec.WindowSpec;
import io.simplesource.saga.client.api.KafkaSagaAPI;
import io.simplesource.saga.model.api.SagaAPI;
import io.simplesource.saga.model.serdes.SagaSerdes;
import io.simplesource.saga.model.specs.SagaSpec;
import io.simplesource.saga.shared.topics.TopicConfig;
import io.simplesource.saga.shared.topics.TopicConfigBuilder;
import io.simplesource.saga.shared.topics.TopicTypes;

import java.util.Collections;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;

public final class SagaClientBuilder<A> {

    private final KafkaConfig kafkaConfig;
    private ScheduledExecutorService scheduler = null;
    private SagaSerdes<A> serdes = null;
    private TopicConfig topicConfig = null;
    private String clientId = null;
    private WindowSpec windowSpec = new WindowSpec(3600L);

    public SagaClientBuilder(Function<KafkaConfig.Builder, KafkaConfig.Builder> configBuildSteps) {
        KafkaConfig.Builder configBuilder = new KafkaConfig.Builder();
        this.kafkaConfig = configBuildSteps.apply(configBuilder).build();
    }

    public SagaClientBuilder<A> withSerdes(SagaSerdes<A> serdes) {
        this.serdes = serdes;
        return this;
    }

    public SagaClientBuilder<A> withResponseWindow(WindowSpec windowSpec) {
        this.windowSpec = windowSpec;
        return this;
    }

    public SagaClientBuilder<A> withScheduler(ScheduledExecutorService scheduler) {
        this.scheduler = scheduler;
        return this;
    }

    public SagaClientBuilder<A> withTopicConfig(TopicConfigBuilder.BuildSteps topicBuildFn) {
        TopicConfigBuilder tcb = new TopicConfigBuilder(TopicTypes.SagaTopic.client, Collections.emptyMap(), Collections.emptyMap());
        topicBuildFn.applyStep(tcb);
        this.topicConfig = tcb.build();
        return this;
    }

    public SagaClientBuilder<A> withClientId(String clientId) {
        this.clientId = clientId;
        return this;
    }

    public SagaAPI<A> build() {
        requireNonNull(serdes, "Serdes have not been defined");
        requireNonNull(topicConfig, "TopicConfig has not been defined");
        requireNonNull(clientId, "ClientId has not been defined");
        if (scheduler == null)
            scheduler = Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("SagaApp-scheduler"));

        SagaSpec<A> sagaSpec =new SagaSpec<>(serdes, windowSpec);
        return new KafkaSagaAPI<>(sagaSpec, kafkaConfig, topicConfig, clientId, scheduler);
    }
}
