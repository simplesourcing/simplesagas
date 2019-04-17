package io.simplesource.saga.saga.app;

import io.simplesource.saga.model.messages.SagaStateTransition;
import io.simplesource.saga.model.saga.RetryStrategy;
import io.simplesource.saga.model.saga.SagaId;
import io.simplesource.saga.model.specs.ActionSpec;
import io.simplesource.saga.model.specs.SagaSpec;
import io.simplesource.saga.saga.internal.SagaContext;
import io.simplesource.saga.saga.internal.SagaTopologyBuilder;
import io.simplesource.saga.shared.kafka.KafkaPublisher;
import io.simplesource.saga.shared.properties.PropertiesBuilder;
import io.simplesource.saga.shared.topics.*;
import io.simplesource.saga.shared.app.StreamAppUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
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
 * <p>
 * We can typically create and run an instance of a Saga application with code like this:
 * <pre>{@code
 * SagaApp.of(
 *         sagaSpec,
 *         actionSpec,
 *         sagaTopicBuilder -> sagaTopicBuilder.withDefaultTopicSpec(partitions, replication, retentionInDays)
 *         configBuilder -> configBuilder)
 *     .withActions("event_sourcing_account", "event_sourcing_auction", "event_sourcing_user", "async_payment")
 *     .withRetryStrategy(RetryStrategy.repeat(3, Duration.ofSeconds(10)))
 *     .run(streamAppConfig);
 * }</pre>
 * <p>
 * The saga coordinator only needs to know which action types are supported, and how to serialize the action requests.
 * <p>
 * It does not need to know anything about the action processor implementations. Neither does it need to know the
 * topology of the saga graph. This is left up to the client to define dynamically.
 *
 * @param <A> a representation of an action command that is shared across all actions in the saga. This is typically a generic type, such as Json, or if using Avro serialization, SpecificRecord or GenericRecord
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

    private SagaApp(SagaSpec<A> sagaSpec,
                    ActionSpec<A> actionSpec,
                    TopicConfigBuilder.BuildSteps sagaTopicBuildSteps,
                    PropertiesBuilder.BuildSteps propertiesBuildSteps) {
        this.sagaSpec = sagaSpec;
        this.actionSpec = actionSpec;
        this.sagaTopicBuildSteps = sagaTopicBuildSteps;
        this.propertiesBuildSteps = propertiesBuildSteps;
    }

    /**
     * Static constructor for a {@code SagaApp} saga coordinator application.
     *
     * @param <A>               a representation of an action command that is shared across all actions in the saga. This is typically a generic type, such as Json, or if using Avro serialization, SpecificRecord or GenericRecord
     * @param sagaSpec          Information common to all sagas, such as Serdes for the saga topics
     * @param actionSpec        Information common to all saga actions and action processors, such as Serdes for the action topics
     * @param topicBuildFn      a function to set topic configuration details incrementally
     * @param propertiesBuildFn a function to set kafka configuration properties incrementally
     * @return the saga app instance
     */
    public static <A> SagaApp<A> of(SagaSpec<A> sagaSpec,
                                    ActionSpec<A> actionSpec,
                                    TopicConfigBuilder.BuildSteps topicBuildFn,
                                    PropertiesBuilder.BuildSteps propertiesBuildFn) {
        return new SagaApp<>(sagaSpec, actionSpec, topicBuildFn, propertiesBuildFn);
    }

    public static <A> SagaApp<A> of(SagaSpec<A> sagaSpec, ActionSpec<A> actionSpec, TopicConfigBuilder.BuildSteps topicBuildFn) {
        return new SagaApp<>(sagaSpec, actionSpec, topicBuildFn, p -> p);
    }

    public static <A> SagaApp<A> of(SagaSpec<A> sagaSpec, ActionSpec<A> actionSpec) {
        return of(sagaSpec, actionSpec, b -> b, p -> p);
    }

    /**
     * Adds a single action, specifying the topic configuration for the request and response topics for that action type
     *
     * @param actionType the action type for the action
     * @param buildFn    a function to set topic configuration details incrementally
     * @return the saga app
     */
    public SagaApp<A> withAction(String actionType, TopicConfigBuilder.BuildSteps buildFn) {
        registerAction(actionType.toLowerCase(), buildFn);
        return this;
    }

    /**
     * Adds multiple actions, specifying the topic configuration for the request and response topics for all action types
     *
     * @param actionTypes the action type to add
     * @param buildFn     a function to set topic configuration details incrementally
     * @return the saga app
     */
    public SagaApp<A> withActions(Collection<String> actionTypes, TopicConfigBuilder.BuildSteps buildFn) {
        actionTypes.forEach(at -> registerAction(at.toLowerCase(), buildFn));
        return this;
    }

    /**
     * Adds multiple actions, using default topic configuration
     *
     * @param actionTypes the action type to add
     * @return the saga app
     */
    public SagaApp<A> withActions(String... actionTypes) {
        return withActions(Arrays.asList(actionTypes), b -> b);
    }

    /**
     * Adds a variable length argument list of action types, specifying the topic configuration for the request and response topics for all action types
     *
     * @param actionTypes the action type to add
     * @param buildFn     a function to set topic configuration details incrementally
     * @return the saga app
     */
    public SagaApp<A> withActions(TopicConfigBuilder.BuildSteps buildFn, String... actionTypes) {
        return withActions(Arrays.asList(actionTypes), buildFn);
    }

    /**
     * Sets an executor for the Saga app. This executor is required for scheduling retries.
     *
     * @param executor the executor
     * @return the saga app
     */
    public SagaApp<A> withExecutor(ScheduledExecutorService executor) {
        this.executor = executor;
        return this;
    }

    /**
     * Sets the retry strategy that applies to all action types, unless specifically overridden
     *
     * @param strategy the strategy
     * @return the saga app
     */
    public SagaApp<A> withRetryStrategy(RetryStrategy strategy) {
        this.defaultRetryStrategy = strategy;
        return this;
    }

    /**
     * Sets the retry strategy for a specific action type
     *
     * @param actionType the action type
     * @param strategy   the strategy
     * @return the saga app
     */
    public SagaApp<A> withRetryStrategy(String actionType, RetryStrategy strategy) {
        this.retryStrategyOverride.put(actionType.toLowerCase(), strategy);
        return this;
    }

    /**
     * Run the SagaApp with the given stream app configuration.
     * <p>
     * This builds the topology, create the saga and action topic streams, and starts the KStream execution
     */
    public void run() {
        PropertiesBuilder.BuildSteps buildSteps = propertiesBuildSteps;

        Properties adminProps = buildSteps
                .build(PropertiesBuilder.Target.AdminClient);

        Properties streamProps = buildSteps
                .build(PropertiesBuilder.Target.StreamApp);

        Properties producerProps = buildSteps
                .build(PropertiesBuilder.Target.Producer);

        Topology topology = buildTopology(topics -> StreamAppUtils.createMissingTopics(adminProps, topics), producerProps);
        logger.info("Topology description {}", topology.describe());

        StreamAppUtils.runStreamApp(streamProps, topology);
    }

    private Topology buildTopology(Consumer<List<TopicCreation>> topicCreator, Properties producerProps) {

        KafkaProducer<byte[], byte[]> producer =
                new KafkaProducer<>(producerProps,
                        Serdes.ByteArray().serializer(),
                        Serdes.ByteArray().serializer());

        KafkaPublisher<SagaId, SagaStateTransition<A>> kafkaPublisher = new KafkaPublisher<>(
                producer,
                sagaSpec.serdes.sagaId(),
                sagaSpec.serdes.transition());

        return buildTopology(topicCreator, kafkaPublisher::send);
    }

    Topology buildTopology(Consumer<List<TopicCreation>> topicCreator, RetryPublisher<A> retryPublisher) {
        final List<TopicCreation> topics = new ArrayList<>();
        TopicConfig sagaTopicConfig = sagaTopicBuildSteps
                .withInitialStep(tcBuilder ->
                        tcBuilder.withTopicBaseName(TopicTypes.SagaTopic.SAGA_BASE_NAME))
                .build(
                        TopicTypes.SagaTopic.all,
                        Collections.singletonMap(
                                org.apache.kafka.common.config.TopicConfig.RETENTION_MS_CONFIG,
                                "-1"),
                        Collections.singletonMap(TopicTypes.SagaTopic.SAGA_STATE, Collections.singletonMap(
                                org.apache.kafka.common.config.TopicConfig.CLEANUP_POLICY_CONFIG,
                                org.apache.kafka.common.config.TopicConfig.CLEANUP_POLICY_COMPACT))
                );

        topics.addAll(sagaTopicConfig.allTopics());

        Map<String, RetryStrategy> retryStrategyMap = getRetryStrategyMap();

        Map<String, TopicNamer> topicNamers = new HashMap<>();
        buildFuncMap.forEach((atlc, buildFn) -> {
            TopicConfig actionTopicConfig = buildFn
                    .withInitialStep(builder -> builder.withTopicBaseName(TopicUtils.actionTopicBaseName(atlc)))
                    .build(TopicTypes.ActionTopic.all);

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
}
