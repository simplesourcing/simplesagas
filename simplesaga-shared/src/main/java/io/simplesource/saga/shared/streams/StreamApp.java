package io.simplesource.saga.shared.streams;

import io.simplesource.saga.shared.kafka.PropertiesBuilder;
import io.simplesource.saga.shared.topics.TopicCreation;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * StreamApp is an action processor that turns saga action requests into Simple Sourcing action requests.
 * It gets the latest sequence id from the stream of command responses for the aggregate.
 * It then forwards the Simple Sourcing command response back to the sagas coordinator.
 *
 * @param <I> Input into
 */
public class StreamApp<I> {

    private final Logger logger = LoggerFactory.getLogger(StreamApp.class);
    private final I streamAppInput;

    private final List<StreamBuildStep> buildSteps = new ArrayList<>();

    public StreamApp(I streamAppInput) {
        this.streamAppInput = streamAppInput;
    }

    public final StreamApp<I> withBuildStep(StreamBuildStep<I> buildStep) {
        buildSteps.add(buildStep);
        return this;
    }

    public StreamBuildResult build(PropertiesBuilder.BuildSteps properties) {

        StreamsBuilder builder = new StreamsBuilder();

        StreamBuildContext<I> context = StreamBuildContext.of(streamAppInput, properties);

        List<StreamBuildSpec> streamBuilders = buildSteps.stream().map(x -> x.applyStep(context)).collect(Collectors.toList());

        logger.info("Expected topics:");
        List<TopicCreation> topicCreations = streamBuilders.stream().flatMap(sb -> sb.topics.stream()).collect(Collectors.toList());

        List<StreamAppUtils.ShutdownHandler> shutdownHandlers = streamBuilders.stream()
                .map(sb -> sb.topologyBuildStep.apply(builder))
                .filter(sh -> sh.isPresent())
                .map(sb -> sb.orElse(null))
                .collect(Collectors.toList());

        Supplier<Topology> topologySupplier = () -> {
            Topology topology = builder.build();
            logger.info("Topology description {}", topology.describe());
            return topology;
        };

        return new StreamBuildResult(topicCreations, topologySupplier, shutdownHandlers);
    }

    /**
     * Run the SourcingApp with the given app configuration.
     * @param properties the app properties
     */
    public void run(PropertiesBuilder.BuildSteps properties) {
        StreamBuildResult streamBuildResult = build(properties);

        // List topic names
        streamBuildResult.topicCreations.stream().map(x -> x.topicName).forEach(logger::info);

        // create missing topics
        Properties props = properties
                .withInitialStep(PropertiesBuilder::withDefaultStreamProps)
                .build();
        StreamAppUtils.createMissingTopics(props, streamBuildResult.topicCreations);
        StreamAppUtils.runStreamApp(props, streamBuildResult.topologySupplier.get());

        // streamBuildResult.shutdownHandlers.forEach(StreamAppUtils.ShutdownHandler::shutDown);
        streamBuildResult.shutdownHandlers.forEach(StreamAppUtils::addShutdownHook);
    }
}
