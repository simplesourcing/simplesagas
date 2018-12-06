package io.simplesource.saga.saga;

import io.simplesource.saga.model.serdes.SagaSerdes;
import io.simplesource.saga.model.specs.SagaSpec;
import io.simplesource.saga.shared.topics.TopicConfigBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;


final public class SagaApp<A> {
    private static Logger logger = LoggerFactory.getLogger(SagaApp.class);
    private final SagaSerdes<A> serdes;

    SagaApp(SagaSpec<A> sagaSpec, TopicConfigBuilder.BuildSteps topicBuildFn) {
        serdes = sagaSpec.serdes;
    }
//
//  private val sagaTopicConfig =
//    TopicConfigBuilder.buildTopics(
//      TopicTypes.SagaTopic.all,
//      Map.empty,
//      Map(
//        TopicTypes.SagaTopic.state -> Map(
//          KafkaTopicConfig.CLEANUP_POLICY_CONFIG -> KafkaTopicConfig.CLEANUP_POLICY_COMPACT,
//        )
//      )
//    )(topicBuildFn)
//
//  final case class ActionProcessorInput(builder: StreamsBuilder,
//                                        sagaRequest: KStream<UUID, messages.SagaRequest<A>>,
//                                        sagaState: KStream<UUID, saga.Saga<A>>,
//                                        sagaStateTransition: KStream<UUID, messages.SagaStateTransition<A>>)
//
//  type ActionProcessor = ActionProcessorInput => Unit
//
//  private var actionProcessors: List<ActionProcessor> = List.empty
//  private var topics: List<TopicCreation>             = TopicCreation.allTopics(sagaTopicConfig)
//
//  def addActionProcessor(actionSpec: ActionProcessorSpec<A>,
//                         buildFn: TopicConfigBuilder.BuildSteps): SagaApp<A> = {
//    val topicConfig = TopicConfigBuilder.buildTopics(TopicTypes.ActionTopic.all, Map.empty)(buildFn)
//    val actionProcessor: ActionProcessor = input => {
//      val ctx = SagaContext(sagaSpec, actionSpec, sagaTopicConfig.namer, topicConfig.namer)
//
//      val actionResponse: KStream<UUID, messages.ActionResponse> =
//        SagaConsumer.actionResponse(actionSpec, topicConfig.namer, input.builder)
//
//      // add in stream transformations
//      SagaStream.addSubTopology(ctx,
//                                input.sagaRequest,
//                                input.sagaStateTransition,
//                                input.sagaState,
//                                actionResponse)
//    }
//
//    actionProcessors = actionProcessor :: actionProcessors
//    topics = topics ++ TopicCreation.allTopics(topicConfig)
//    this
//  }
//
//  def run(appConfig: StreamAppConfig): Unit = {
//    val config: Properties = StreamAppUtils.getConfig(appConfig)
//    StreamAppUtils
//      .addMissingTopics(AdminClient.create(config))(topics.distinct)
//      .all()
//      .get(30L, TimeUnit.SECONDS)
//
//    val builder = new StreamsBuilder()
//    // get input topic streams
//    val topicNamer           = sagaTopicConfig.namer
//    val sagaRequest          = SagaConsumer.sagaRequest(sagaSpec, topicNamer, builder)
//    val sagaState            = SagaConsumer.state(sagaSpec, topicNamer, builder)
//    val sagaStateTransition  = SagaConsumer.stateTransition(sagaSpec, topicNamer, builder)
//    val actionProcessorInput = ActionProcessorInput(builder, sagaRequest, sagaState, sagaStateTransition)
//
//    // add each of the action processors
//    actionProcessors.foreach(_(actionProcessorInput))
//
//    // add the result distributor
//
//    // add result distributor
//    val distCtx = DistributorContext<SagaResponse>(
//      sagaTopicConfig.namer(TopicTypes.SagaTopic.responseTopicMap),
//      DistributorSerdes(serdes.uuid, serdes.response),
//      sagaSpec.responseWindow,
//      _.sagaId
//    )
//
//    val topicNames: KStream<UUID, String> = ResultDistributor.resultTopicMapStream(distCtx, builder)
//    val sagaResponse                      = SagaConsumer.sagaResponse(sagaSpec, topicNamer, builder)
//    ResultDistributor.distribute(distCtx, sagaResponse, topicNames)
//
//    // build the topology
//    val topology = builder.build()
//    logger.info("Topology description {}", topology.describe())
//    StreamAppUtils.runStreamApp(config, topology)
//  }
//}
}



//final case class SagaApp<A>(sagaSpec: SagaSpec<A>, topicBuildFn: TopicConfigBuilder.BuildSteps) {
//  private val logger = LoggerFactory.getLogger(classOf<SagaApp<A>>)
//
//  private val sagaTopicConfig =
//    TopicConfigBuilder.buildTopics(
//      TopicTypes.SagaTopic.all,
//      Map.empty,
//      Map(
//        TopicTypes.SagaTopic.state -> Map(
//          KafkaTopicConfig.CLEANUP_POLICY_CONFIG -> KafkaTopicConfig.CLEANUP_POLICY_COMPACT,
//        )
//      )
//    )(topicBuildFn)
//  private val serdes = sagaSpec.serdes
//
//  final case class ActionProcessorInput(builder: StreamsBuilder,
//                                        sagaRequest: KStream<UUID, messages.SagaRequest<A>>,
//                                        sagaState: KStream<UUID, saga.Saga<A>>,
//                                        sagaStateTransition: KStream<UUID, messages.SagaStateTransition<A>>)
//
//  type ActionProcessor = ActionProcessorInput => Unit
//
//  private var actionProcessors: List<ActionProcessor> = List.empty
//  private var topics: List<TopicCreation>             = TopicCreation.allTopics(sagaTopicConfig)
//
//  def addActionProcessor(actionSpec: ActionProcessorSpec<A>,
//                         buildFn: TopicConfigBuilder.BuildSteps): SagaApp<A> = {
//    val topicConfig = TopicConfigBuilder.buildTopics(TopicTypes.ActionTopic.all, Map.empty)(buildFn)
//    val actionProcessor: ActionProcessor = input => {
//      val ctx = SagaContext(sagaSpec, actionSpec, sagaTopicConfig.namer, topicConfig.namer)
//
//      val actionResponse: KStream<UUID, messages.ActionResponse> =
//        SagaConsumer.actionResponse(actionSpec, topicConfig.namer, input.builder)
//
//      // add in stream transformations
//      SagaStream.addSubTopology(ctx,
//                                input.sagaRequest,
//                                input.sagaStateTransition,
//                                input.sagaState,
//                                actionResponse)
//    }
//
//    actionProcessors = actionProcessor :: actionProcessors
//    topics = topics ++ TopicCreation.allTopics(topicConfig)
//    this
//  }
//
//  def run(appConfig: StreamAppConfig): Unit = {
//    val config: Properties = StreamAppUtils.getConfig(appConfig)
//    StreamAppUtils
//      .addMissingTopics(AdminClient.create(config))(topics.distinct)
//      .all()
//      .get(30L, TimeUnit.SECONDS)
//
//    val builder = new StreamsBuilder()
//    // get input topic streams
//    val topicNamer           = sagaTopicConfig.namer
//    val sagaRequest          = SagaConsumer.sagaRequest(sagaSpec, topicNamer, builder)
//    val sagaState            = SagaConsumer.state(sagaSpec, topicNamer, builder)
//    val sagaStateTransition  = SagaConsumer.stateTransition(sagaSpec, topicNamer, builder)
//    val actionProcessorInput = ActionProcessorInput(builder, sagaRequest, sagaState, sagaStateTransition)
//
//    // add each of the action processors
//    actionProcessors.foreach(_(actionProcessorInput))
//
//    // add the result distributor
//
//    // add result distributor
//    val distCtx = DistributorContext<SagaResponse>(
//      sagaTopicConfig.namer(TopicTypes.SagaTopic.responseTopicMap),
//      DistributorSerdes(serdes.uuid, serdes.response),
//      sagaSpec.responseWindow,
//      _.sagaId
//    )
//
//    val topicNames: KStream<UUID, String> = ResultDistributor.resultTopicMapStream(distCtx, builder)
//    val sagaResponse                      = SagaConsumer.sagaResponse(sagaSpec, topicNamer, builder)
//    ResultDistributor.distribute(distCtx, sagaResponse, topicNames)
//
//    // build the topology
//    val topology = builder.build()
//    logger.info("Topology description {}", topology.describe())
//    StreamAppUtils.runStreamApp(config, topology)
//  }
//}
