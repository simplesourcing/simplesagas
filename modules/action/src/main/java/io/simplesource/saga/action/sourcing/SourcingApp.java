package io.simplesource.saga.action.sourcing;


import io.simplesource.kafka.model.CommandResponse;
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

import java.util.*;
import java.util.concurrent.TimeUnit;

public final class SourcingApp<A> {
    private final TopicConfig actionTopicConfig;
    private final ActionProcessorSpec<A> actionSpec;
    private final Logger logger;

    private final List<Command> commands = new ArrayList<>();
    private final List<TopicCreation> topicCreations;

    @Value
    static final class CommandInput<A> {
        final StreamsBuilder builder;
        final KStream<UUID, ActionRequest<A>> actionRequests;
        final KStream<UUID, ActionResponse> actionResponses;
    }

    static interface Command<A> {
        void applyCommandInput(CommandInput<A> input);
    }


//  private var topics
//    : List<TopicCreation> = TopicCreation.allTopics(actionTopicConfig) //   topicNames.ActionTopic.all.map(TopicCreation(actionTopicConfig))


    public SourcingApp(ActionSerdes<A> actionSerdes , TopicConfigBuilder.BuildSteps topicBuildFn) {
        actionTopicConfig =
                TopicConfigBuilder.buildTopics(TopicTypes.ActionTopic.all, new HashMap<>(), new HashMap<>(), topicBuildFn);
        actionSpec = new ActionProcessorSpec(actionSerdes);
        logger = LoggerFactory.getLogger(SourcingApp.class);
        topicCreations = TopicCreation.allTopics(actionTopicConfig);
    }

    public <I, K, C>  SourcingApp<A> addCommand(CommandSpec<A, I, K, C> cSpec, TopicConfigBuilder.BuildSteps topicBuildSteps) {
        TopicConfig commandTopicConfig = TopicConfigBuilder.buildTopics(TopicTypes.CommandTopic.all, new HashMap<>(), new HashMap<>(), topicBuildSteps);
        SourcingContext<A, I, K, C> actionContext = new SourcingContext<>(actionSpec, cSpec, actionTopicConfig.namer, commandTopicConfig.namer);

        Command<A> command = input -> {
            KStream<K, CommandResponse> commandResponses = CommandConsumer.commandResponseStream(cSpec, commandTopicConfig.namer, input.builder);
      SourcingStream.addSubTopology(actionContext,
                                    input.actionRequests,
                                    input.actionResponses,
                                    commandResponses);
        };

        commands.add(command);
        topicCreations.addAll(TopicCreation.allTopics(commandTopicConfig));
        return this;
    }


    public void run(StreamAppConfig appConfig) {
        Properties config = StreamAppConfig.getConfig(appConfig);

        try {
            StreamAppUtils
              .addMissingTopics(AdminClient.create(config), topicCreations)
              .all()
              .get(30L, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new RuntimeException("Unable to create all the topics");
        }

        StreamsBuilder builder = new StreamsBuilder();

        KStream<UUID, ActionRequest<A>> actionRequests  =
      ActionConsumer.actionRequestStream(actionSpec, actionTopicConfig.namer, builder);
        KStream<UUID, ActionResponse> actionResponses  =
      ActionConsumer.actionResponseStream(actionSpec, actionTopicConfig.namer, builder);

    CommandInput<A> commandInput = new CommandInput(builder, actionRequests, actionResponses);


    for (Command<A> c: commands) {
        c.applyCommandInput(commandInput);
    }

    Topology topology = builder.build();
    logger.info("Topology description {}", topology.describe());
    StreamAppUtils.runStreamApp(config, topology);
  }


//                          topicBuildFn: TopicConfigBuilder.BuildSteps): SourcingApp<A> = {

//  final case class CommandInput(builder: StreamsBuilder,
//                                actionRequests: KStream<UUID, ActionRequest<A>>,
//                                actionResponses: KStream<UUID, ActionResponse>)
//
//  type Command = CommandInput => CommandInput
//
//  private var commands: List<Command> = List.empty
//  private var topics
//    : List<TopicCreation> = TopicCreation.allTopics(actionTopicConfig) //   topicNames.ActionTopic.all.map(TopicCreation(actionTopicConfig))
//
//  def addCommand<I, K, C>(cSpec: CommandSpec<A, I, K, C>,
//                          topicBuildFn: TopicConfigBuilder.BuildSteps): SourcingApp<A> = {
//    val commandTopicConfig =
//      TopicConfigBuilder.buildTopics(TopicTypes.CommandTopic.all, Map.empty)(topicBuildFn)
//    val actionContext = SourcingContext(actionSpec, cSpec, actionTopicConfig.namer, commandTopicConfig.namer)
//    val command: Command = input => {
//      val commandResponses =
//        CommandConsumer.commandResponseStream(cSpec, commandTopicConfig.namer, input.builder)
//      SourcingStream.addSubTopology(actionContext,
//                                    input.actionRequests,
//                                    input.actionResponses,
//                                    commandResponses)
//    }
//    commands = command :: commands
//    topics = topics ++ TopicCreation.allTopics(commandTopicConfig)
//    this
//  }
//
//  def run(appConfig: StreamAppConfig): Unit = {
//    val config: Properties = StreamAppUtils.getConfig(appConfig)
//
//    StreamAppUtils
//      .addMissingTopics(AdminClient.create(config))(topics.distinct)
//      .all()
//      .get(30L, TimeUnit.SECONDS)
//
//    val builder = new StreamsBuilder()
//    val actionRequests: KStream<UUID, messages.ActionRequest<A>> =
//      ActionConsumer.actionRequestStream(actionSpec, actionTopicConfig.namer, builder)
//    val actionResponses: KStream<UUID, messages.ActionResponse> =
//      ActionConsumer.actionResponseStream(actionSpec, actionTopicConfig.namer, builder)
//
//    val commandInput = CommandInput(builder, actionRequests, actionResponses)
//    commands.foreach(_(commandInput))
//    val topology = builder.build()
//    logger.info("Topology description {}", topology.describe())
//    StreamAppUtils.runStreamApp(config, topology)
//  }
}
