package saga
import java.util.concurrent.TimeUnit
import java.util.{Properties, UUID}

import model.serdes.SagaSerdes
import model.specs.{ActionProcessorSpec, SagaSpec}
import org.apache.kafka.common.config.{TopicConfig => KafkaTopicConfig}
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.kstream.KStream
import org.slf4j.LoggerFactory
import saga.app.{SagaConsumer, SagaContext, SagaStream}
import model.{messages, saga, topics => topicNames}
import shared.utils.TopicConfigurer.{TopicCreation, getTopics}
import shared.utils.{StreamAppConfig, StreamAppUtils, TopicConfigBuilder}

final case class SagaApp[A](serdes: SagaSerdes[A], topicBuildFn: TopicConfigBuilder.BuildSteps) {
  private val logger = LoggerFactory.getLogger(classOf[SagaApp[A]])

  private val sagaTopicConfig =
    TopicConfigBuilder.buildTopics(
      topicNames.SagaTopic.all,
      Map.empty,
      Map(
        topicNames.SagaTopic.state -> Map(
          KafkaTopicConfig.CLEANUP_POLICY_CONFIG -> KafkaTopicConfig.CLEANUP_POLICY_COMPACT,
        )
      )
    )(topicBuildFn)
  private val sagaSpec = SagaSpec[A](serdes, sagaTopicConfig)

  final case class ActionProcessorInput(builder: StreamsBuilder,
                                        sagaRequest: KStream[UUID, messages.SagaRequest[A]],
                                        sagaState: KStream[UUID, saga.Saga[A]],
                                        sagaStateTransition: KStream[UUID, messages.SagaStateTransition[A]])

  type ActionProcessor = ActionProcessorInput => Unit

  private var actionProcessors: List[ActionProcessor] = List.empty
  private var topics: List[TopicCreation]             = getTopics(sagaSpec.topicConfig)

  def addActionProcessor(actionSpec: ActionProcessorSpec[A],
                         buildFn: TopicConfigBuilder.BuildSteps): SagaApp[A] = {
    val topicConfig = TopicConfigBuilder.buildTopics(topicNames.ActionTopic.all, Map.empty)(buildFn)
    val actionProcessor: ActionProcessor = input => {
      val ctx = SagaContext(sagaSpec, actionSpec, topicConfig.namer)

      val actionResponse: KStream[UUID, messages.ActionResponse] =
        SagaConsumer.actionResponse(actionSpec, topicConfig.namer, input.builder)

      // add in stream transformations
      SagaStream.addSubTopology(ctx,
                                input.sagaRequest,
                                input.sagaStateTransition,
                                input.sagaState,
                                actionResponse)
    }

    actionProcessors = actionProcessor :: actionProcessors
    topics = topics ++ getTopics(topicConfig)
    this
  }

  def run(appConfig: StreamAppConfig): Unit = {
    val config: Properties = StreamAppUtils.getConfig(appConfig)
    StreamAppUtils
      .addMissingTopics(AdminClient.create(config))(topics.distinct)
      .all()
      .get(30L, TimeUnit.SECONDS)

    val builder = new StreamsBuilder()
    // get input topic streams
    val sagaRequest          = SagaConsumer.sagaRequest(sagaSpec, builder)
    val sagaState            = SagaConsumer.state(sagaSpec, builder)
    val sagaStateTransition  = SagaConsumer.stateTransition(sagaSpec, builder)
    val actionProcessorInput = ActionProcessorInput(builder, sagaRequest, sagaState, sagaStateTransition)

    // add each of the action processors
    actionProcessors.foreach(_(actionProcessorInput))

    // build the topology
    val topology = builder.build()
    logger.info("Topology description {}", topology.describe())
    StreamAppUtils.runStreamApp(config, topology)
  }
}
