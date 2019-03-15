package io.simplesource.saga.saga;

import io.simplesource.saga.model.messages.ActionResponse;
import io.simplesource.saga.model.messages.SagaRequest;
import io.simplesource.saga.model.messages.SagaResponse;
import io.simplesource.saga.model.messages.SagaStateTransition;
import io.simplesource.saga.model.saga.Saga;
import io.simplesource.saga.model.serdes.SagaSerdes;
import io.simplesource.saga.model.specs.ActionProcessorSpec;
import io.simplesource.saga.model.specs.SagaSpec;
import io.simplesource.saga.saga.app.*;
import io.simplesource.saga.shared.topics.*;
import io.simplesource.saga.shared.utils.StreamAppConfig;
import io.simplesource.saga.shared.utils.StreamAppUtils;
import lombok.Value;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
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

    @Value
    private final static class ActionProcessorInput<A> {
        public final StreamsBuilder builder;
        public final KStream<UUID, SagaRequest<A>> sagaRequest;
        public final KStream<UUID, Saga<A>> sagaState;
        public final KStream<UUID, SagaStateTransition> sagaStateTransition;
    }

    private interface ActionProcessor<A> {
        void apply(ActionProcessorInput<A> input);
    }

    private static Logger logger = LoggerFactory.getLogger(SagaApp.class);
    private final SagaSpec<A> sagaSpec;
    private final SagaSerdes<A> serdes;
    private final TopicConfig sagaTopicConfig;
    private final List<ActionProcessor<A>> actionProcessors = new ArrayList<>();
    private final List<TopicCreation> topics;

    public SagaApp(SagaSpec<A> sagaSpec, TopicConfigBuilder.BuildSteps topicBuildFn) {
        serdes = sagaSpec.serdes;
        this.sagaSpec = sagaSpec;
        sagaTopicConfig = TopicConfigBuilder.buildTopics(
                TopicTypes.SagaTopic.all,
                Collections.emptyMap(),
                Collections.singletonMap(TopicTypes.SagaTopic.state, Collections.singletonMap(
                        org.apache.kafka.common.config.TopicConfig.CLEANUP_POLICY_CONFIG,
                        org.apache.kafka.common.config.TopicConfig.CLEANUP_POLICY_COMPACT
                )),
                topicBuildFn);

        topics = TopicCreation.allTopics(sagaTopicConfig);
    }

    public SagaApp<A> addActionProcessor(ActionProcessorSpec<A> actionSpec, TopicConfigBuilder.BuildSteps buildFn) {
        TopicConfig topicConfig = TopicConfigBuilder.buildTopics(TopicTypes.ActionTopic.all, Collections.emptyMap(), Collections.emptyMap(), buildFn);

        ActionProcessor<A> actionProcessor = input -> {
            SagaContext<A> ctx = new SagaContext<>(sagaSpec, actionSpec, sagaTopicConfig.namer, topicConfig.namer);
            KStream<UUID, ActionResponse> actionResponse = SagaConsumer.actionResponse(actionSpec, topicConfig.namer, input.builder);
            SagaStream.addSubTopology(ctx,
                    input.sagaRequest,
                    input.sagaStateTransition,
                    input.sagaState,
                    actionResponse);
        };
        actionProcessors.add(actionProcessor);
        topics.addAll(TopicCreation.allTopics(topicConfig));
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

        StreamsBuilder builder = new StreamsBuilder();
        // get input topic streams
        TopicNamer topicNamer = sagaTopicConfig.namer;
        KStream<UUID, SagaRequest<A>> sagaRequest = SagaConsumer.sagaRequest(sagaSpec, topicNamer, builder);
        KStream<UUID, Saga<A>> sagaState = SagaConsumer.state(sagaSpec, topicNamer, builder);
        KStream<UUID, SagaStateTransition> sagaStateTransition = SagaConsumer.stateTransition(sagaSpec, topicNamer, builder);
        ActionProcessorInput<A> actionProcessorInput = new ActionProcessorInput<>(builder, sagaRequest, sagaState, sagaStateTransition);
        actionProcessors.forEach(p -> p.apply(actionProcessorInput));

        DistributorContext<SagaResponse> distCtx = new DistributorContext<>(
                new DistributorSerdes<>(serdes.uuid(), serdes.response()),
                sagaTopicConfig.namer.apply(TopicTypes.SagaTopic.responseTopicMap),
                sagaSpec.responseWindow,
                response -> response.sagaId);

        KStream<UUID, String> topicNames = ResultDistributor.resultTopicMapStream(distCtx, builder);
        KStream<UUID, SagaResponse> sagaResponse = SagaConsumer.sagaResponse(sagaSpec, topicNamer, builder);
        ResultDistributor.distribute(distCtx, sagaResponse, topicNames);

        // build the topology
        Topology topology = builder.build();
        logger.info("Topology description {}", topology.describe());
        StreamAppUtils.runStreamApp(config, topology);
    }
}
