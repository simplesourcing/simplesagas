package io.simplesource.saga.saga;

import io.simplesource.saga.model.messages.SagaStateTransition;
import io.simplesource.saga.model.saga.RetryStrategy;
import io.simplesource.saga.model.saga.SagaId;
import io.simplesource.saga.model.specs.ActionSpec;
import io.simplesource.saga.model.specs.SagaSpec;
import io.simplesource.saga.saga.app.RetryPublisher;
import io.simplesource.saga.saga.app.SagaContext;
import io.simplesource.saga.saga.app.SagaTopologyBuilder;
import io.simplesource.saga.shared.kafka.AsyncKafkaPublisher;
import io.simplesource.saga.shared.kafka.KafkaUtils;
import io.simplesource.saga.shared.kafka.PropertiesBuilder;
import io.simplesource.saga.shared.topics.*;
import io.simplesource.saga.shared.streams.StreamAppConfig;
import io.simplesource.saga.shared.streams.StreamAppUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Consumer;


/**
 * SagaApp (the "Saga Coordinator") accepts a dependency graph of saga actions.
 * It then executes these actions in the order specified by the dependency graph.
 * An action is executed once its dependencies have successfully executed.
 * Actions that are not dependent on one another can be executed in parallel.
 * <p>
 * Action execution involves submitting to the action request Kafka topic and waiting for it to finish
 * by listening to the action response topic.
 * <p>
 * The result of action execution leads to a saga state transition. When this happens the next action(s)
 * can be submitted, or if all actions have completed, finishing the saga and publishing to the saga
 * response topic.
 * <p>
 * If any of the actions fail, the actions that are already completed are undone, if an undo action is defined.
 *
 * @param <A> action type.
 */
final public class SagaApp<A> {

    private static Logger logger = LoggerFactory.getLogger(SagaApp.class);
    private final SagaSpec<A> sagaSpec;
    private final ActionSpec<A> actionSpec;

    private final Map<String, RetryStrategy> retryStrategyOverride = new HashMap<>();
    private RetryStrategy defaultRetryStrategy = RetryStrategy.failFast();
    private final Map<String, TopicConfigBuilder.BuildSteps> buildFuncMap = new HashMap<>();
    private final TopicConfigBuilder.BuildSteps sagaTopicBuildSteps;
    private final PropertiesBuilder.BuildSteps propertiesBuildSteps;
    private ScheduledExecutorService executor;

    private SagaApp(SagaSpec<A> sagaSpec, ActionSpec<A> actionSpec, TopicConfigBuilder.BuildSteps sagaTopicBuildSteps, PropertiesBuilder.BuildSteps propertiesBuildSteps) {
        this.sagaSpec = sagaSpec;
        this.actionSpec = actionSpec;
        this.sagaTopicBuildSteps = sagaTopicBuildSteps;
        this.propertiesBuildSteps = propertiesBuildSteps;
    }

    public static <A> SagaApp<A> of(SagaSpec<A> sagaSpec, ActionSpec<A> actionSpec, TopicConfigBuilder.BuildSteps topicBuildFn, PropertiesBuilder.BuildSteps propertiesBuildFn) {
        return new SagaApp<>(sagaSpec, actionSpec, topicBuildFn, propertiesBuildFn);
    }

    public static <A> SagaApp<A> of(SagaSpec<A> sagaSpec, ActionSpec<A> actionSpec, TopicConfigBuilder.BuildSteps topicBuildFn) {
        return new SagaApp<>(sagaSpec, actionSpec, topicBuildFn, p -> p);
    }

    public static <A> SagaApp<A> of(SagaSpec<A> sagaSpec, ActionSpec<A> actionSpec) {
        return of(sagaSpec, actionSpec, b -> b, p -> p);
    }

    public SagaApp<A> withAction(String actionType, TopicConfigBuilder.BuildSteps buildFn) {
        registerAction(actionType.toLowerCase(), buildFn);
        return this;
    }

    public SagaApp<A> withActions(Collection<String> actionTypes, TopicConfigBuilder.BuildSteps buildFn) {
        actionTypes.forEach(at -> registerAction(at.toLowerCase(), buildFn));
        return this;
    }

    public SagaApp<A> withActions(String... actionTypes) {
        return withActions(Arrays.asList(actionTypes), b -> b);
    }

    public SagaApp<A> withExecutor(ScheduledExecutorService executor) {
        this.executor = executor;
        return this;
    }

    public SagaApp<A> withRetryStrategy(RetryStrategy strategy) {
        this.defaultRetryStrategy = strategy;
        return this;
    }

    public SagaApp<A> withRetryStrategy(String actionType, RetryStrategy strategy) {
        this.retryStrategyOverride.put(actionType.toLowerCase(), strategy);
        return this;
    }

    private void registerAction(String atlc, TopicConfigBuilder.BuildSteps buildFn) {
        buildFuncMap.put(atlc, buildFn);

    }

    private Map<String, RetryStrategy> getRetryStrategyMap() {
        Map<String, RetryStrategy> retryStrategyMap = new HashMap<>();

        buildFuncMap.keySet().forEach(atlc -> {
            retryStrategyMap.put(atlc, retryStrategyOverride.getOrDefault(atlc, this.defaultRetryStrategy));
        });
        return retryStrategyMap;
    }

    /**
     * Run the SagaApp with the given app configuration.
     *
     * @param appConfig app configuration.
     */
    public void run(StreamAppConfig appConfig) {
        PropertiesBuilder.BuildSteps buildSteps = propertiesBuildSteps.withNextStep(p -> p.withStreamAppConfig(appConfig));
        Properties config = buildSteps.build();
        Topology topology = buildTopology(topics -> StreamAppUtils.createMissingTopics(config, topics), buildSteps);
        logger.info("Topology description {}", topology.describe());
        StreamAppUtils.runStreamApp(config, topology);
    }

    Topology buildTopology(Consumer<List<TopicCreation>> topicCreator, PropertiesBuilder.BuildSteps propertyBuildSteps) {
        Properties producerProps = propertyBuildSteps
                .withInitialStep(pb -> pb.withProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true"))
                .build();

        KafkaProducer<byte[], byte[]> producer =
                new KafkaProducer<>(producerProps,
                        Serdes.ByteArray().serializer(),
                        Serdes.ByteArray().serializer());

        AsyncKafkaPublisher<SagaId, SagaStateTransition<A>> kafkaPublisher = new AsyncKafkaPublisher<>(
                producer,
                sagaSpec.serdes.sagaId(),
                sagaSpec.serdes.transition());

        return buildTopology(topicCreator, kafkaPublisher::send);
    }

    Topology buildTopology(Consumer<List<TopicCreation>> topicCreator) {
        return buildTopology(topicCreator, p -> p);
    }

    Topology buildTopology(Consumer<List<TopicCreation>> topicCreator, RetryPublisher<A> retryPublisher) {
        final List<TopicCreation> topics = new ArrayList<>();
        TopicConfig sagaTopicConfig = TopicConfigBuilder.build(
                TopicTypes.SagaTopic.all,
                Collections.emptyMap(),
                Collections.singletonMap(TopicTypes.SagaTopic.SAGA_STATE, Collections.singletonMap(
                        org.apache.kafka.common.config.TopicConfig.CLEANUP_POLICY_CONFIG,
                        org.apache.kafka.common.config.TopicConfig.CLEANUP_POLICY_COMPACT
                )),
                sagaTopicBuildSteps.withInitialStep(tcBuilder ->
                        tcBuilder.withTopicBaseName(TopicTypes.SagaTopic.SAGA_BASE_NAME)));

        topics.addAll(sagaTopicConfig.allTopics());

        Map<String, RetryStrategy> retryStrategyMap = getRetryStrategyMap();

        Map<String, TopicNamer> topicNamers = new HashMap<>();
        buildFuncMap.forEach((atlc, buildFn) -> {
            TopicConfig actionTopicConfig = TopicConfigBuilder.build(
                    TopicTypes.ActionTopic.all,
                    buildFn.withInitialStep(builder -> builder.withTopicBaseName(TopicUtils.actionTopicBaseName(atlc))));

            topics.addAll(TopicCreation.allTopics(actionTopicConfig));
            topicNamers.put(atlc, actionTopicConfig.namer);
        });

        topicCreator.accept(topics);

        ScheduledExecutorService usedExecutor = executor != null ? executor : Executors.newScheduledThreadPool(1);

        SagaContext<A> sagaContext = new SagaContext<>(
                sagaSpec,
                actionSpec,
                sagaTopicConfig.namer,
                topicNamers,
                retryStrategyMap,
                retryPublisher,
                usedExecutor);

        StreamsBuilder builder = new StreamsBuilder();
        SagaTopologyBuilder.addSubTopology(sagaContext, builder);
        return builder.build();
    }
}
