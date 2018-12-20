package io.simplesource.saga.action.async;

import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import io.simplesource.saga.action.common.ActionConsumer;
import io.simplesource.saga.model.messages.ActionRequest;
import io.simplesource.saga.model.messages.ActionResponse;
import io.simplesource.saga.model.serdes.ActionSerdes;
import io.simplesource.saga.model.specs.ActionProcessorSpec;
import io.simplesource.saga.shared.topics.TopicConfig;
import io.simplesource.saga.shared.topics.TopicConfigBuilder;
import io.simplesource.saga.shared.topics.TopicCreation;
import io.simplesource.saga.shared.topics.TopicTypes;
import io.simplesource.saga.shared.utils.StreamAppConfig;
import io.simplesource.saga.shared.utils.StreamAppUtils;
import lombok.Value;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class AsyncApp<A> {

    private final Logger logger = LoggerFactory.getLogger(AsyncApp.class);
    private final List<AsyncTransformer<A>> transformers = new ArrayList<>();
    private final List<Supplier<Integer>> closeHandlers = new ArrayList<>();

    private final List<TopicCreation> expectedTopics;
    private final TopicConfig actionTopicConfig;
    private final ActionProcessorSpec<A> actionSpec;
    private ScheduledExecutorService executor;

    @Value
    private static class AsyncTransformerInput<A> {
        final StreamsBuilder builder;
        final KStream<UUID, ActionRequest<A>> actionRequests;
        final KStream<UUID, ActionResponse> actionResponses;
        final ScheduledExecutorService executor;
    }

    private interface AsyncTransformer<A> {
        Function<Properties, AsyncTransform.AsyncPipe> apply(AsyncTransformerInput<A> input);
    }

    public AsyncApp(ActionSerdes<A> actionSerdes, TopicConfigBuilder.BuildSteps topicBuildFn) {
        List<String> expectedTopicList = new ArrayList<>(TopicTypes.ActionTopic.all);
        expectedTopicList.add(TopicTypes.ActionTopic.requestUnprocessed);

        actionTopicConfig = TopicConfigBuilder.buildTopics(expectedTopicList, new HashMap<>(), new HashMap<>(), topicBuildFn);
        actionSpec = new ActionProcessorSpec<A>(actionSerdes);
        expectedTopics = TopicCreation.allTopics(actionTopicConfig);

    }

    public <I, K, O, R> AsyncApp<A> addAsync(AsyncSpec<A, I, K, O, R> spec) {
        AsyncTransformer<A> transformer = input -> {
            AsyncContext<A, I, K, O, R> ctx = new AsyncContext<>(actionSpec, actionTopicConfig.namer, spec, input.executor);
            // join the action request with corresponding prior io.simplesource.io.simplesource.saga.user.saga.user.command responses
            AsyncStream.addSubTopology(ctx, input.actionRequests, input.actionResponses);

            return AsyncTransform.async(ctx);
        };
        transformers.add(transformer);
        spec.outputSpec.ifPresent(oSpec -> expectedTopics.addAll(oSpec.getTopicCreations()));
        return this;
    }

    public AsyncApp<A> addExecutor(ScheduledExecutorService executor) {
        this.executor = executor;
        return this;
    }

    public AsyncApp<A>  addCloseHandler(Supplier<Integer> handler) {
        closeHandlers.add(handler);
        return this;
    }

    public void run(StreamAppConfig appConfig) {
        Properties config = StreamAppConfig.getConfig(appConfig);

        try {
            StreamAppUtils
                    .addMissingTopics(AdminClient.create(config), expectedTopics)
                    .all()
                    .get(30L, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new RuntimeException("Unable to create missing topics");
        }

        StreamsBuilder builder = new StreamsBuilder();
        KStream<UUID, ActionRequest<A>> actionRequests =
                ActionConsumer.actionRequestStream(actionSpec, actionTopicConfig.namer, builder);
        KStream<UUID, ActionResponse> actionResponses =
                ActionConsumer.actionResponseStream(actionSpec, actionTopicConfig.namer, builder);

        ScheduledExecutorService usedExecutor = executor != null ? executor : Executors.newScheduledThreadPool(1);
        AsyncTransformerInput<A> commandInput = new AsyncTransformerInput<>(builder, actionRequests, actionResponses, usedExecutor);
        List<AsyncTransform.AsyncPipe> pipes = transformers.stream().map(x -> {
            Function<Properties, AsyncTransform.AsyncPipe> propsToPipe = x.apply(commandInput);
            return propsToPipe.apply(config);
        }).collect(Collectors.toList());

        Topology topology = builder.build();
        logger.info("Topology description {}", topology.describe());
        StreamAppUtils.runStreamApp(config, topology);

        StreamAppUtils.addShutdownHook(() -> {
            logger.info("Shutting down AsyncTransformers");
            pipes.forEach(AsyncTransform.AsyncPipe::close);
            closeHandlers.forEach(Supplier::get);

            StreamAppUtils.shutdownExecutorService(usedExecutor);
        });
    }
}
