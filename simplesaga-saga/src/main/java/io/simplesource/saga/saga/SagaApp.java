package io.simplesource.saga.saga;

import io.simplesource.saga.model.specs.ActionSpec;
import io.simplesource.saga.model.specs.SagaSpec;
import io.simplesource.saga.saga.app.SagaContext;
import io.simplesource.saga.saga.app.SagaTopologyBuilder;
import io.simplesource.saga.shared.topics.*;
import io.simplesource.saga.shared.streams.StreamAppConfig;
import io.simplesource.saga.shared.streams.StreamAppUtils;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

/**
 * SagaApp (the "Saga Coordinator") accepts a dependency graph of saga actions.
 * It then executes these actions in the order specified by the dependency graph.
 * An action is executed once its dependencies have successfully executed.
 * Actions that are not dependent on one another can be executed in parallel.
 *
 * Action execution involves submitting to the action request Kafka topic and waiting for it to finish
 * by listening to the action response topic.
 *
 * The result of action execution leads to a saga state transition. When this happens the next action(s)
 * can be submitted, or if all actions have completed, finishing the saga and publishing to the saga
 * response topic.
 *
 * If any of the actions fail, the actions that are already completed are undone, if an undo action is defined.
 *
 * @param <A> action type.
 */
final public class SagaApp<A> {

    private static Logger logger = LoggerFactory.getLogger(SagaApp.class);
    private final SagaSpec<A> sagaSpec;
    private final ActionSpec<A> actionSpec;
    private final TopicConfig sagaTopicConfig;
    private final List<TopicCreation> topics = new ArrayList<>();
    private final Map<String, TopicNamer> topicNamers = new HashMap<>();

    private SagaApp(SagaSpec<A> sagaSpec, ActionSpec<A> actionSpec, TopicConfigBuilder.BuildSteps topicBuildFn) {
        this.sagaSpec = sagaSpec;
        this.actionSpec = actionSpec;
        sagaTopicConfig = TopicConfigBuilder.build(
                TopicTypes.SagaTopic.all,
                Collections.emptyMap(),
                Collections.singletonMap(TopicTypes.SagaTopic.SAGA_STATE, Collections.singletonMap(
                        org.apache.kafka.common.config.TopicConfig.CLEANUP_POLICY_CONFIG,
                        org.apache.kafka.common.config.TopicConfig.CLEANUP_POLICY_COMPACT
                )),
                topicBuildFn.withInitialStep(tcBuilder ->
                        tcBuilder.withTopicBaseName(TopicTypes.SagaTopic.SAGA_BASE_NAME)));

        topics.addAll(sagaTopicConfig.allTopics());
    }

    public static <A> SagaApp<A> of(SagaSpec<A> sagaSpec, ActionSpec<A> actionSpec, TopicConfigBuilder.BuildSteps topicBuildFn) {
        return new SagaApp<>(sagaSpec, actionSpec, topicBuildFn);
    }

    public static <A> SagaApp<A> of(SagaSpec<A> sagaSpec, ActionSpec<A> actionSpec) {
        return of(sagaSpec, actionSpec, b -> b);
    }

    public SagaApp<A> withAction(String actionType, TopicConfigBuilder.BuildSteps buildFn) {
        String atlc = actionType.toLowerCase();
        if (topicNamers.containsKey(atlc)) throw new RuntimeException(String.format("ActionType has already been added for action '%s'", actionType));

        TopicConfig actionTopicConfig = TopicConfigBuilder.build(
                TopicTypes.ActionTopic.all,
                buildFn.withInitialStep(builder -> builder.withTopicBaseName(TopicUtils.actionTopicBaseName(atlc))));

        topics.addAll(TopicCreation.allTopics(actionTopicConfig));
        topicNamers.put(atlc, actionTopicConfig.namer);
        return this;
    }

    public SagaApp<A> withActions(Collection<String> actionTypes, TopicConfigBuilder.BuildSteps buildFn) {
        Set<String> actionTypeSet = actionTypes
                .stream()
                .map(String::toLowerCase)
                .collect(Collectors.toSet());

        // throw if any one is already there
        Set<String> intersection = new HashSet<>(actionTypeSet);
        intersection.retainAll(topicNamers.keySet());
        if (!intersection.isEmpty())
            throw new RuntimeException(String.format("%d actions are already present.", intersection.size()));

        actionTypeSet.forEach(at -> withAction(at, buildFn));
        return this;
    }

    public SagaApp<A> withActions(String... actionTypes) {
        return withActions(Arrays.asList(actionTypes), b -> b);
    }

    /**
     * Run the SagaApp with the given app configuration.
     * @param appConfig app configuration.
     */
    public void run(StreamAppConfig appConfig) {
        Properties config = StreamAppConfig.getConfig(appConfig);
        StreamAppUtils.createMissingTopics(config, topics);
        Topology topology = buildTopology();
        logger.info("Topology description {}", topology.describe());
        StreamAppUtils.runStreamApp(config, topology);
    }

    Topology buildTopology() {
        SagaContext<A> sagaContext = new SagaContext<>(sagaSpec, actionSpec, sagaTopicConfig.namer, topicNamers);
        StreamsBuilder builder = new StreamsBuilder();
        SagaTopologyBuilder.addSubTopology(sagaContext, builder);
        return builder.build();
    }
}
