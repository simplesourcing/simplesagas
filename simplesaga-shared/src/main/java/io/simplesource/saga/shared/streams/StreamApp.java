package io.simplesource.saga.shared.streams;

import io.simplesource.saga.shared.topics.TopicCreation;
import lombok.Value;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * StreamApp is an action processor that turns saga action requests into Simple Sourcing action requests.
 * It gets the latest sequence id from the stream of command responses for the aggregate.
 * It then forwards the Simple Sourcing command response back to the sagas coordinator.
 *
 * @param <I> Input into
 */
public final class StreamApp<I> {

    private final Logger logger = LoggerFactory.getLogger(StreamApp.class);
    private final I streamBuildInput;

    public interface BuildStep<I> {
        StreamBuildSpec applyStep(StreamBuildContext<I> context);
    }

    @Value
    public class StreamBuild {
        public final List<TopicCreation> topicCreations;
        public final Supplier<Topology> topologySupplier;
        public final List<StreamAppUtils.ShutdownHandler> shutdownHandlers;
    }

    private final List<BuildStep> buildSteps = new ArrayList<>();

    public StreamApp(I streamBuildInput) {
        this.streamBuildInput = streamBuildInput;
    }

    public final StreamApp<I> withBuildStep(BuildStep<I> buildStep) {
        buildSteps.add(buildStep);
        return this;
    }

    public StreamBuild build(Properties properties) {

        StreamsBuilder builder = new StreamsBuilder();

        StreamBuildContext<I> context = new StreamBuildContext<I>(streamBuildInput, properties);

        List<StreamBuildSpec> streamBuilders = buildSteps.stream().map(x -> x.applyStep(context)).collect(Collectors.toList());

        logger.info("Expected topics:");
        List<TopicCreation> topicCreations = streamBuilders.stream().flatMap(sb -> sb.topics.stream()).collect(Collectors.toList());

        List<StreamAppUtils.ShutdownHandler> shutdownHandlers = streamBuilders.stream()
                .map(sb -> sb.topologyBuildStep.apply(builder))
                .collect(Collectors.toList());

        Supplier<Topology> topologySupplier = () -> {
            Topology topology = builder.build();
            logger.info("Topology description {}", topology.describe());
            return topology;
        };

        return new StreamBuild(topicCreations, topologySupplier, shutdownHandlers);
    }

    /**
     * Run the SourcingApp with the given app configuration.
     * @param appConfig app configuration.
     */
    public void run(StreamAppConfig appConfig) {
        Properties config = StreamAppConfig.getConfig(appConfig);

        StreamBuild streamBuild = build(config);

        // List topic names
        streamBuild.topicCreations.stream().map(x -> x.topicName).forEach(logger::info);

        try {
            StreamAppUtils
                    .createMissingTopics(AdminClient.create(config), streamBuild.topicCreations)
                    .all()
                    .get(30L, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new RuntimeException("Unable to create all the topics");
        }

        StreamAppUtils.runStreamApp(config, streamBuild.topologySupplier.get());

        streamBuild.shutdownHandlers.forEach(StreamAppUtils.ShutdownHandler::shutDown);
    }
}
