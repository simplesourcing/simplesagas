package saga
import java.util
import java.util.concurrent.TimeUnit
import java.util.{Collections, Properties, UUID}

import io.simplesource.saga.model.messages.{ActionResponse, SagaRequest, SagaResponse, SagaStateTransition}
import io.simplesource.saga.model.saga.Saga
import io.simplesource.saga.model.serdes.{SagaClientSerdes, SagaSerdes}
import io.simplesource.saga.model.specs.{ActionProcessorSpec, SagaSpec}
import io.simplesource.saga.saga.app._
import io.simplesource.saga.shared.topics.{TopicConfigBuilder, TopicCreation, TopicTypes}
import io.simplesource.saga.shared.utils.{StreamAppConfig, StreamAppUtils}
import org.apache.kafka.common.config.{TopicConfig => KafkaTopicConfig}
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.kstream.KStream
import org.slf4j.LoggerFactory

final case class SagaApp[A](sagaSpec: SagaSpec[A], topicBuildFn: TopicConfigBuilder.BuildSteps) {
  private val logger = LoggerFactory.getLogger(classOf[SagaApp[A]])

  private val sagaTopicConfig =
    TopicConfigBuilder.buildTopics(
      TopicTypes.SagaTopic.all,
      new util.HashMap(),
      Collections.singletonMap(
        TopicTypes.SagaTopic.state,
        Collections.singletonMap(
          KafkaTopicConfig.CLEANUP_POLICY_CONFIG,
          KafkaTopicConfig.CLEANUP_POLICY_COMPACT,
        )
      ),
      topicBuildFn
    )
  private val serdes: SagaClientSerdes[A] = sagaSpec.serdes

  final case class ActionProcessorInput(builder: StreamsBuilder,
                                        sagaRequest: KStream[UUID, SagaRequest[A]],
                                        sagaState: KStream[UUID, Saga[A]],
                                        sagaStateTransition: KStream[UUID, SagaStateTransition])

  type ActionProcessor = ActionProcessorInput => Unit

  private val actionProcessors: util.List[ActionProcessor] = new util.ArrayList[ActionProcessor]()
  private val topics: util.List[TopicCreation] = {
    val list = new util.ArrayList[TopicCreation]()
    list.addAll(TopicCreation.allTopics(sagaTopicConfig))
    list
  }

  def addActionProcessor(actionSpec: ActionProcessorSpec[A],
                         buildFn: TopicConfigBuilder.BuildSteps): SagaApp[A] = {
    val topicConfig = TopicConfigBuilder.buildTopics(TopicTypes.ActionTopic.all,
                                                     Collections.emptyMap(),
                                                     Collections.emptyMap(),
                                                     buildFn)
    val actionProcessor: ActionProcessor = input => {
      val ctx = new SagaContext(sagaSpec, actionSpec, sagaTopicConfig.namer, topicConfig.namer)

      val actionResponse: KStream[UUID, ActionResponse] =
        SagaConsumer.actionResponse(actionSpec, topicConfig.namer, input.builder)

      // add in stream transformations
      SagaStream.addSubTopology(ctx,
                                input.sagaRequest,
                                input.sagaStateTransition,
                                input.sagaState,
                                actionResponse)
    }

    actionProcessors.add(actionProcessor)
    topics.addAll(TopicCreation.allTopics(topicConfig))
    this
  }

  def run(appConfig: StreamAppConfig): Unit = {
    val config: Properties = StreamAppConfig.getConfig(appConfig)
    StreamAppUtils
      .addMissingTopics(AdminClient.create(config), topics)
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
    actionProcessors.forEach(_.apply(actionProcessorInput))

    // add the result distributor

    // add result distributor
    val disSerdes = new DistributorSerdes[SagaResponse](serdes.uuid, serdes.response)
    val topicName = sagaTopicConfig.namer.apply(TopicTypes.SagaTopic.responseTopicMap)
    val distCtx: DistributorContext[SagaResponse] = new DistributorContext[SagaResponse](
      disSerdes,
      topicName,
      sagaSpec.responseWindow,
      (resp: SagaResponse) => resp.sagaId
    )

    val topicNames: KStream[UUID, String] = ResultDistributor.resultTopicMapStream(distCtx, builder)
    val sagaResponse                      = SagaConsumer.sagaResponse(sagaSpec, topicNamer, builder)
    ResultDistributor.distribute(distCtx, sagaResponse, topicNames)

    // build the topology
    val topology = builder.build()
    logger.info("Topology description {}", topology.describe())
    StreamAppUtils.runStreamApp(config, topology)
  }
}
