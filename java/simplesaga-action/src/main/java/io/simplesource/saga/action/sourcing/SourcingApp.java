package io.simplesource.saga.action.sourcing;

import io.simplesource.saga.action.internal.ActionTopologyBuilder;
import io.simplesource.saga.action.internal.SourcingStream;
import io.simplesource.saga.model.serdes.ActionSerdes;
import io.simplesource.saga.model.specs.ActionProcessorSpec;
import io.simplesource.saga.shared.topics.TopicConfig;
import io.simplesource.saga.shared.topics.TopicConfigBuilder;
import io.simplesource.saga.shared.topics.TopicCreation;
import io.simplesource.saga.shared.topics.TopicTypes;
import io.simplesource.saga.shared.utils.StreamAppConfig;
import io.simplesource.saga.shared.utils.StreamAppUtils;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.streams.Topology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * SourcingApp is an action processor that turns saga action requests into Simple Sourcing action requests.
 * It gets the latest sequence id from the stream of command responses for the aggregate.
 * It then forwards the Simple Sourcing command response back to the sagas coordinator.
 *
 * @param <A> action command type.
 */
public final class SourcingApp<A> {

    private final Logger logger = LoggerFactory.getLogger(SourcingApp.class);

    private final TopicConfig actionTopicConfig;
    private final ActionProcessorSpec<A> actionSpec;
    private final List<TopicCreation> topicCreations;
    private final ActionTopologyBuilder<A> topologyBuilder;

    /**
     * Constructor.
     * @param actionSerdes action serdes.
     * @param actionTopicBuildSteps action topic builder.
     */
    public SourcingApp(ActionSerdes<A> actionSerdes, TopicConfigBuilder.BuildSteps actionTopicBuildSteps) {
        actionTopicConfig =
                TopicConfigBuilder.buildTopics(TopicTypes.ActionTopic.all, Collections.emptyMap(), Collections.emptyMap(), actionTopicBuildSteps);
        actionSpec = new ActionProcessorSpec<>(actionSerdes);
        topicCreations = TopicCreation.allTopics(actionTopicConfig);
        topologyBuilder = new ActionTopologyBuilder<>(actionSpec, actionTopicConfig);
    }

    /**
     * Add a command handler to the SourcingApp.
     * @param cSpec command spec.
     * @param commandTopicBuilder command topic builder.
     * @param <I> intermediate type.
     * @param <K> key type.
     * @param <C> command type.
     */
    public <I, K, C> SourcingApp<A> addCommand(CommandSpec<A, I, K, C> cSpec, TopicConfigBuilder.BuildSteps commandTopicBuilder) {
        TopicConfig commandTopicConfig = TopicConfigBuilder.buildTopics(TopicTypes.CommandTopic.all, Collections.emptyMap(), Collections.emptyMap(), commandTopicBuilder);
        topicCreations.addAll(TopicCreation.allTopics(commandTopicConfig));

        topologyBuilder.onBuildTopology((topologyContext) -> {
            SourcingContext<A, I, K, C> sourcing = new SourcingContext<>(actionSpec, cSpec, actionTopicConfig.namer, commandTopicConfig.namer);
            SourcingStream.addSubTopology(topologyContext, sourcing);
        });

        return this;
    }

    /**
     * Run the SourcingApp with the given app configuration.
     * @param appConfig app configuration.
     */
    public void run(StreamAppConfig appConfig) {
        Properties config = StreamAppConfig.getConfig(appConfig);

        logger.info("Expected topics:");
        topicCreations.stream().map(x -> x.topicName).forEach(logger::info);

        try {
            StreamAppUtils
                    .createMissingTopics(AdminClient.create(config), topicCreations)
                    .all()
                    .get(30L, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new RuntimeException("Unable to create all the topics");
        }

        Topology topology = buildTopology(appConfig);
        logger.info("Topology description {}", topology.describe());
        StreamAppUtils.runStreamApp(config, topology);
    }

    Topology buildTopology(StreamAppConfig appConfig) {
        return topologyBuilder.build(appConfig);
    }
}
