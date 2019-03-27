package io.simplesource.saga.saga;

import io.simplesource.saga.model.specs.ActionProcessorSpec;
import io.simplesource.saga.model.specs.SagaSpec;
import io.simplesource.saga.saga.app.SagaContext;
import io.simplesource.saga.saga.app.SagaTopologyBuilder;
import io.simplesource.saga.saga.app.SagaStream;
import io.simplesource.saga.shared.topics.TopicConfig;
import io.simplesource.saga.shared.topics.TopicConfigBuilder;
import io.simplesource.saga.shared.topics.TopicCreation;
import io.simplesource.saga.shared.topics.TopicTypes;
import io.simplesource.saga.shared.streams.StreamAppConfig;
import io.simplesource.saga.shared.streams.StreamAppUtils;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.streams.Topology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.TimeUnit;

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
    private final ActionProcessorSpec<A> actionSpec;
    private final TopicConfig sagaTopicConfig;
    private final SagaTopologyBuilder<A> topologyBuilder;
    private final List<TopicCreation> topics;
    private final Set<String> actionTypes = new HashSet<>();

    public SagaApp(SagaSpec<A> sagaSpec, ActionProcessorSpec<A> actionSpec, TopicConfigBuilder.BuildSteps topicBuildFn) {
        this.sagaSpec = sagaSpec;
        this.actionSpec = actionSpec;
        sagaTopicConfig = TopicConfigBuilder.build(
                TopicTypes.SagaTopic.all,
                Collections.emptyMap(),
                Collections.singletonMap(TopicTypes.SagaTopic.SAGA_STATE, Collections.singletonMap(
                        org.apache.kafka.common.config.TopicConfig.CLEANUP_POLICY_CONFIG,
                        org.apache.kafka.common.config.TopicConfig.CLEANUP_POLICY_COMPACT
                )),
                topicBuildFn.withInitialStep(b -> b.withTopicBaseName(TopicTypes.SagaTopic.SAGA_BASE_NAME)));
        topologyBuilder = new SagaTopologyBuilder<>(sagaSpec, sagaTopicConfig);
        topics = sagaTopicConfig.allTopics();
    }

    public SagaApp<A> addActionProcessor(String actionType, TopicConfigBuilder.BuildSteps buildFn) {
        if (actionTypes.contains(actionType)) throw new RuntimeException("ActionType has already been added");

        TopicConfigBuilder.BuildSteps initialBuildStep = builder -> builder.withTopicBaseName(actionType.toLowerCase());

        TopicConfig topicConfig = TopicConfigBuilder.build(TopicTypes.ActionTopic.all, buildFn.withInitialStep(initialBuildStep));
        topics.addAll(TopicCreation.allTopics(topicConfig));
        actionTypes.add(actionType);

        topologyBuilder.onBuildTopology((topologyContext) -> {
            SagaContext<A> saga = new SagaContext<>(sagaSpec, actionSpec, sagaTopicConfig.namer, topicConfig.namer);
            SagaStream.addSubTopology(topologyContext, saga);
        });
        return this;
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
        return topologyBuilder.build();
    }
}
