package io.simplesource.saga.action.sourcing


import io.simplesource.saga.model.messages.ActionRequest;
import io.simplesource.saga.model.messages.ActionResponse;
import io.simplesource.saga.model.serdes.ActionSerdes;
import io.simplesource.saga.model.specs.ActionProcessorSpec;
import io.simplesource.saga.shared.topics.TopicConfig;
import io.simplesource.saga.shared.topics.TopicConfigBuilder;
import io.simplesource.saga.shared.topics.TopicTypes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;

final class SourcingApp<A> {
    private final TopicConfig actionTopicConfig;
    private final ActionProcessorSpec<A> actionSpec;
    private final Logger logger;

    public SourcingApp(ActionSerdes<A> actionSerdes , Function<TopicConfigBuilder, TopicConfigBuilder> topicBuildFn) {
        actionTopicConfig =
                TopicConfigBuilder.buildTopics(TopicTypes.ActionTopic.all, new HashMap<>(), new HashMap<>(), topicBuildFn);
        actionSpec = new ActionProcessorSpec(actionSerdes);
        logger = LoggerFactory.getLogger(SourcingApp.class);
    }

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
