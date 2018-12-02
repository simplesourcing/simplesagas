package io.simplesource.saga.action.async;

import java.util.*;
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
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class AsyncApp<A> {

    Logger logger = LoggerFactory.getLogger(AsyncApp.class);
    List<String> expectedTopicList = new ArrayList<>();
    List<AsyncTransformer> transformers = new ArrayList<>();
    List<Supplier<Integer>> closeHandlers = new ArrayList<>();

    private final List<TopicCreation> expectedTopics;
    private final TopicConfig actionTopicConfig;
    private final ActionProcessorSpec<A> actionSpec;


    @Value
    static class AsyncTransformerInput<A> {
        final StreamsBuilder builder;
        final KStream<UUID, ActionRequest<A>> actionRequests;
        final KStream<UUID, ActionResponse> actionResponses;
    }

    interface AsyncTransformer<A> {
        Function<Properties, AsyncTransform.AsyncPipe> apply(AsyncTransformerInput<A> input);
    }

    AsyncApp(ActionSerdes<A> actionSerdes, TopicConfigBuilder.BuildSteps topicBuildFn) {
        expectedTopicList.addAll(TopicTypes.ActionTopic.all);
        expectedTopicList.add(TopicTypes.ActionTopic.requestUnprocessed);

        actionTopicConfig = TopicConfigBuilder.buildTopics(expectedTopicList, new HashMap<>(), new HashMap<>(), topicBuildFn);
        actionSpec = new ActionProcessorSpec<A>(actionSerdes);
        expectedTopics = TopicCreation.allTopics(actionTopicConfig)

    }

    <I, K, O, R> AsyncApp<A> addAsync(AsyncSpec<A, I, K, O, R> spec) {
        AsyncContext<A, I, K, O, R> ctx = new AsyncContext<>(actionSpec, actionTopicConfig.namer, spec);
        AsyncTransformer<A> transformer = input -> {

            // join the action request with corresponding prior command responses
            AsyncStream.addSubTopology(ctx, input.actionRequests, input.actionResponses);

            return AsyncTransform.async(ctx);
        };
        transformers.add(transformer);
        spec.outputSpec.ifPresent(oSpec -> {
            expectedTopics.addAll(oSpec.getTopicCreations());
        });

        return this;
    }

  void addCloseHandler(Supplier<Integer> handler) {
    closeHandlers.add(handler);
  }


  void run(StreamAppConfig appConfig) {

      Properties  config = StreamAppConfig.getConfig(appConfig);

    StreamAppUtils
      .addMissingTopics(AdminClient.create(config), expectedTopics)
      .all()
      .get(30L, TimeUnit.SECONDS);

      StreamsBuilder builder = new StreamsBuilder();
    KStream<UUID, ActionRequest<A>> actionRequests =
      ActionConsumer.actionRequestStream(actionSpec, actionTopicConfig.namer, builder);
      KStream<UUID, ActionResponse>  actionResponses =
      ActionConsumer.actionResponseStream(actionSpec, actionTopicConfig.namer, builder);

      AsyncTransformerInput<A> commandInput          = new AsyncTransformerInput<>(builder, actionRequests, actionResponses);
      List<AsyncTransform.AsyncPipe> pipes  = Collectors.toList(transformers.stream().map(x -> x.apply(commandInput).apply(config));

    val topology = builder.build()
    logger.info("Topology description {}", topology.describe())
    StreamAppUtils.runStreamApp(config, topology)

    Runtime.getRuntime.addShutdownHook(new Thread {
      override def run(): Unit = {
        logger.info("Shutting down AsyncTransformers")
        pipes.foreach(_.close())

        closeHandlers.foreach { handler =>
          handler.apply()
        }
      }
    })
  }

}


//  private final TopicConfig actionTopicConfig;
//  private final ActionProcessorSpec<A> actionSpec;
//  private final Logger logger;
//
//  private final List<Command> commands = new ArrayList<>();
//  private final List<TopicCreation> topicCreations;
//
//  @Value
//  static final class CommandInput<A> {
//    final StreamsBuilder builder;
//    final KStream<UUID, ActionRequest<A>> actionRequests;
//    final KStream<UUID, ActionResponse> actionResponses;
//  }
//
//  static interface Command<A> {
//    void applyCommandInput(CommandInput<A> input);
//  }
//
//

//final case class AsyncApp<A>(actionSerdes: ActionSerdes<A>, topicBuildFn: TopicConfigBuilder.BuildSteps) {
//  private val logger = LoggerFactory.getLogger(classOf<AsyncApp<A>>)
//
//  final case class AsyncTransformerInput(builder: StreamsBuilder,
//                                         actionRequests: KStream<UUID, ActionRequest<A>>,
//                                         actionResponses: KStream<UUID, ActionResponse>)
//
//  val expectedTopicList = TopicTypes.ActionTopic.requestUnprocessed :: TopicTypes.ActionTopic.all
//
//  private val actionTopicConfig = TopicConfigBuilder.buildTopics(expectedTopicList, Map.empty)(topicBuildFn)
//  private val actionSpec        = ActionProcessorSpec<A>(actionSerdes)
//
//  type AsyncTransformer = AsyncTransformerInput => Properties => AsyncPipe
//
//  private var transformers: List<AsyncTransformer> = List.empty
//  private var expectedTopics                       = TopicCreation.allTopics(actionTopicConfig)
//
//  private var closeHandlers: List<() => Unit> = List.empty
//
//  def addAsync<I, K, O, R>(spec: AsyncSpec<A, I, K, O, R>)(
//      implicit executionContext: ExecutionContext): AsyncApp<A> = {
//    val ctx = AsyncContext(actionSpec, actionTopicConfig.namer, spec)
//    val transformer: AsyncTransformer = input => {
//
//      // join the action request with corresponding prior command responses
//      AsyncStream.addSubTopology<A, I, K, O, R>(ctx, input.actionRequests, input.actionResponses)
//
//      new AsyncPipe { override def close(): Unit = {} }
//      AsyncTransform.async(ctx)
//    }
//    transformers = transformer :: transformers
//    expectedTopics = expectedTopics ++ spec.outputSpec.fold(List.empty<TopicCreation>)(_.topicCreation)
//    this
//  }
//
//  def addCloseHandler(handler: => Unit): Unit = {
//    closeHandlers = (() => handler) :: closeHandlers
//  }
//
//  def run(appConfig: StreamAppConfig): Unit = {
//    val config = StreamAppUtils.getConfig(appConfig)
//
//    StreamAppUtils
//      .addMissingTopics(AdminClient.create(config))(expectedTopics.distinct)
//      .all()
//      .get(30L, TimeUnit.SECONDS)
//
//    val builder = new StreamsBuilder()
//    val actionRequests: KStream<UUID, messages.ActionRequest<A>> =
//      ActionConsumer.actionRequestStream(actionSpec, actionTopicConfig.namer, builder)
//    val actionResponses: KStream<UUID, messages.ActionResponse> =
//      ActionConsumer.actionResponseStream(actionSpec, actionTopicConfig.namer, builder)
//
//    val commandInput          = AsyncTransformerInput(builder, actionRequests, actionResponses)
//    val pipes: Seq<AsyncPipe> = transformers.map(x => x(commandInput)(config))
//
//    val topology = builder.build()
//    logger.info("Topology description {}", topology.describe())
//    StreamAppUtils.runStreamApp(config, topology)
//
//    Runtime.getRuntime.addShutdownHook(new Thread {
//      override def run(): Unit = {
//        logger.info("Shutting down AsyncTransformers")
//        pipes.foreach(_.close())
//
//        closeHandlers.foreach { handler =>
//          handler.apply()
//        }
//      }
//    })
//  }
//}
