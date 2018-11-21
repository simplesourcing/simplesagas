package saga
import java.util.concurrent.TimeUnit
import java.util.{Properties, UUID}

import model.specs.{ActionProcessorSpec, SagaSpec}
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.kstream.KStream
import org.slf4j.LoggerFactory
import saga.app.{SagaConsumer, SagaContext, SagaStream}
import model.{messages, saga}
import shared.utils.TopicConfigurer.{TopicCreation, getTopics}
import shared.utils.{StreamAppConfig, StreamAppUtils}

final case class SagaApp[A](sagaSpec: SagaSpec[A]) {
  private val logger = LoggerFactory.getLogger(classOf[SagaApp[A]])

  final case class ActionProcessorInput(builder: StreamsBuilder,
                                        sagaRequest: KStream[UUID, messages.SagaRequest[A]],
                                        sagaState: KStream[UUID, saga.Saga[A]],
                                        sagaStateTransition: KStream[UUID, messages.SagaStateTransition[A]])

  type ActionProcessor = ActionProcessorInput => Unit

  private var actionProcessors: List[ActionProcessor] = List.empty
  private var topics: List[TopicCreation]             = getTopics(sagaSpec.topicConfig)

  def addActionProcessor(actionSpec: ActionProcessorSpec[A]): SagaApp[A] = {
    val actionProcessor: ActionProcessor = input => {
      val ctx = SagaContext(sagaSpec, actionSpec)

      val actionResponse: KStream[UUID, messages.ActionResponse] =
        SagaConsumer.actionResponse(actionSpec, input.builder)

      // add in stream transformations
      SagaStream.addSubTopology(ctx,
                                input.sagaRequest,
                                input.sagaStateTransition,
                                input.sagaState,
                                actionResponse)
    }

    actionProcessors = actionProcessor :: actionProcessors
    topics = topics ++ getTopics(actionSpec.topicConfig)
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
