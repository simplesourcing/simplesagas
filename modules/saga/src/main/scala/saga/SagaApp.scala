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
import model.{messages, saga}
import topics.topics.{TopicConfigBuilder, TopicCreation, TopicTypes}
import topics.utils._

final case class SagaApp[A](serdes: SagaSerdes[A], topicBuildFn: TopicConfigBuilder.BuildSteps) {
  private val logger = LoggerFactory.getLogger(classOf[SagaApp[A]])

  private val sagaTopicConfig =
    TopicConfigBuilder.buildTopics(
      TopicTypes.SagaTopic.all,
      Map.empty,
      Map(
        TopicTypes.SagaTopic.state -> Map(
          KafkaTopicConfig.CLEANUP_POLICY_CONFIG -> KafkaTopicConfig.CLEANUP_POLICY_COMPACT,
        )
      )
    )(topicBuildFn)
  private val sagaSpec = SagaSpec[A](serdes)

  final case class ActionProcessorInput(builder: StreamsBuilder,
                                        sagaRequest: KStream[UUID, messages.SagaRequest[A]],
                                        sagaState: KStream[UUID, saga.Saga[A]],
                                        sagaStateTransition: KStream[UUID, messages.SagaStateTransition[A]])

  type ActionProcessor = ActionProcessorInput => Unit

  private var actionProcessors: List[ActionProcessor] = List.empty
  private var topics: List[TopicCreation]             = TopicCreation.allTopics(sagaTopicConfig)

  def addActionProcessor(actionSpec: ActionProcessorSpec[A],
                         buildFn: TopicConfigBuilder.BuildSteps): SagaApp[A] = {
    val topicConfig = TopicConfigBuilder.buildTopics(TopicTypes.ActionTopic.all, Map.empty)(buildFn)
    val actionProcessor: ActionProcessor = input => {
      val ctx = SagaContext(sagaSpec, actionSpec, sagaTopicConfig.namer, topicConfig.namer)

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
    topics = topics ++ TopicCreation.allTopics(topicConfig)
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
    val topicNamer           = sagaTopicConfig.namer
    val sagaRequest          = SagaConsumer.sagaRequest(sagaSpec, topicNamer, builder)
    val sagaState            = SagaConsumer.state(sagaSpec, topicNamer, builder)
    val sagaStateTransition  = SagaConsumer.stateTransition(sagaSpec, topicNamer, builder)
    val actionProcessorInput = ActionProcessorInput(builder, sagaRequest, sagaState, sagaStateTransition)

    // add each of the action processors
    actionProcessors.foreach(_(actionProcessorInput))

    // build the topology
    val topology = builder.build()
    logger.info("Topology description {}", topology.describe())
    StreamAppUtils.runStreamApp(config, topology)
  }
}
