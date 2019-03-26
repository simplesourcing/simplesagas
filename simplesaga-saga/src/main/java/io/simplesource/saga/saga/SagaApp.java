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

import java.util.Collections;
import java.util.List;
import java.util.Properties;
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
    private final TopicConfig sagaTopicConfig;
    private final SagaTopologyBuilder<A> topologyBuilder;
    private final List<TopicCreation> topics;

    public SagaApp(SagaSpec<A> sagaSpec, TopicConfigBuilder.BuildSteps topicBuildFn) {
        this.sagaSpec = sagaSpec;
        sagaTopicConfig = TopicConfigBuilder.build(
                TopicTypes.SagaTopic.all,
                Collections.emptyMap(),
                Collections.singletonMap(TopicTypes.SagaTopic.state, Collections.singletonMap(
                        org.apache.kafka.common.config.TopicConfig.CLEANUP_POLICY_CONFIG,
                        org.apache.kafka.common.config.TopicConfig.CLEANUP_POLICY_COMPACT
                )),
                topicBuildFn);
        topologyBuilder = new SagaTopologyBuilder<>(sagaSpec, sagaTopicConfig);
        topics = TopicCreation.allTopics(sagaTopicConfig);
    }

    public SagaApp<A> addActionProcessor(ActionProcessorSpec<A> actionSpec, TopicConfigBuilder.BuildSteps buildFn) {
        TopicConfig topicConfig = TopicConfigBuilder.build(TopicTypes.ActionTopic.all, buildFn);
        topics.addAll(TopicCreation.allTopics(topicConfig));

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
        try {
            StreamAppUtils
                    .createMissingTopics(AdminClient.create(config), topics)
                    .all()
                    .get(30L, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new RuntimeException("Unable to add missing topics", e);
        }
        Topology topology = buildTopology();
        logger.info("Topology description {}", topology.describe());
        StreamAppUtils.runStreamApp(config, topology);
    }

    Topology buildTopology() {
        return topologyBuilder.build();
    }
}
