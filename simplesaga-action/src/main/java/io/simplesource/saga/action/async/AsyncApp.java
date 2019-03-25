package io.simplesource.saga.action.async;

import io.simplesource.saga.action.internal.ActionTopologyBuilder;
import io.simplesource.saga.action.internal.AsyncStream;
import io.simplesource.saga.action.internal.AsyncPipe;
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

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * AsyncApp is an Action Processor that executes arbitrary asynchronous functions as action requests.
 *
 * @param <A> common representation form for all action commands (typically Json / GenericRecord for Avro).
 */
public final class AsyncApp<A> {

    private final Logger logger = LoggerFactory.getLogger(AsyncApp.class);
    private final List<Supplier<Integer>> closeHandlers = new ArrayList<>();

    private final List<TopicCreation> expectedTopics;
    private final TopicConfig actionTopicConfig;
    private final ActionProcessorSpec<A> actionSpec;
    private ScheduledExecutorService executor;
    private ActionTopologyBuilder<A> topologyBuilder;
    private Set<ExecutorService> executorServices = new HashSet<>();

    public AsyncApp(ActionSerdes<A> actionSerdes, TopicConfigBuilder.BuildSteps topicBuildFn) {
        List<String> expectedTopicList = new ArrayList<>(TopicTypes.ActionTopic.all);
        expectedTopicList.add(TopicTypes.ActionTopic.requestUnprocessed);

        actionTopicConfig = TopicConfigBuilder.buildTopics(expectedTopicList, new HashMap<>(), new HashMap<>(), topicBuildFn);
        actionSpec = new ActionProcessorSpec<>(actionSerdes);
        topologyBuilder = new ActionTopologyBuilder<>(actionSpec, actionTopicConfig);
        expectedTopics = TopicCreation.allTopics(actionTopicConfig);

    }

    public <D, K, O, R> AsyncApp<A> addAsync(AsyncSpec<A, D, K, O, R> spec) {
        spec.outputSpec.ifPresent(oSpec -> expectedTopics.addAll(oSpec.topicCreations));

        topologyBuilder.onBuildTopology((topologyContext) -> {
            ScheduledExecutorService usedExecutor = executor != null ? executor : Executors.newScheduledThreadPool(1);
            executorServices.add(usedExecutor);
            AsyncContext<A, D, K, O, R> async = new AsyncContext<>(actionSpec, actionTopicConfig.namer, spec, usedExecutor);
            AsyncPipe pipe = AsyncStream.addSubTopology(topologyContext, async);
            addCloseHandler(() -> {
                pipe.close();
                return 0;
            });
        });

        return this;
    }

    public AsyncApp<A> addExecutor(ScheduledExecutorService executor) {
        this.executor = executor;
        return this;
    }

    public AsyncApp<A> addCloseHandler(Supplier<Integer> handler) {
        closeHandlers.add(handler);
        return this;
    }

    public void run(StreamAppConfig appConfig) {
        Properties config = StreamAppConfig.getConfig(appConfig);

        try {
            StreamAppUtils
                    .createMissingTopics(AdminClient.create(config), expectedTopics)
                    .all()
                    .get(30L, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new RuntimeException("Unable to create missing topics");
        }

        Topology topology = buildTopology(appConfig);
        logger.info("Topology description {}", topology.describe());

        StreamAppUtils.runStreamApp(config, topology);

        StreamAppUtils.addShutdownHook(() -> {
            logger.info("Shutting down async app resources");
            closeHandlers.forEach(Supplier::get);
            executorServices.forEach(StreamAppUtils::shutdownExecutorService);
        });
    }

    public Topology buildTopology(StreamAppConfig appConfig) {
        return topologyBuilder.build(appConfig);
    }

}
