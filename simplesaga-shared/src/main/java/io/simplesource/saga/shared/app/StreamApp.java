package io.simplesource.saga.shared.app;

import io.simplesource.saga.shared.properties.PropertiesBuilder;
import io.simplesource.saga.shared.topics.TopicCreation;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * StreamApp is a generic method for specifying the steps involved in building and running a KStream app that enables:
 * <ul>
 *     <li>Defining some initial configuration that is applicable to the entire build process</li>
 *     <li>Specifying the stream topology for the application in an arbitrary sequence of steps</li>
 *     <li>Specifying which topics are needed to define te topology</li>
 *     <li>Specifying topic configuration information in a flexible way</li>
 *     <li>Collating the list of required topics, and providing and overriding configuration information for their creation</li>
 *     <li>Providing a flexible configuration mechanism for setting Kafka properties</li>
 *     <li>Creating the missing topics prior to running the stream</li>
 *     <li>Running the stream application</li>
 *     <li>Handling and cleanup / freeing of resources post termination of the stream</li>
 * </ul>
 * It is a functional abstraction on top of the Kafka Stream builder DSL that delays the execution of the stream build steps and allows for decorating the process with extra information.
 *
 * It is applied in this project wrapped in a thin layer by the {@link io.simplesource.saga.action.ActionApp}.
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
     * Run the SourcingApp with the given configuration steps
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
