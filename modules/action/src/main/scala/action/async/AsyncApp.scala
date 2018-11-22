package action.async
import java.util.{Properties, UUID}
import java.util.concurrent.TimeUnit

import action.async.AsyncTransform.AsyncPipe
import action.common.ActionConsumer
import model.messages
import model.messages.{ActionRequest, ActionResponse}
import model.serdes.ActionSerdes
import model.specs.ActionProcessorSpec
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.kstream.KStream
import org.slf4j.LoggerFactory
import shared.topics.{TopicConfigBuilder, TopicCreation, TopicTypes}
import shared.utils.{StreamAppConfig, StreamAppUtils}

import scala.concurrent.ExecutionContext

final case class AsyncApp[A](actionSerdes: ActionSerdes[A], topicBuildFn: TopicConfigBuilder.BuildSteps) {
  private val logger = LoggerFactory.getLogger(classOf[AsyncApp[A]])

  final case class AsyncTransformerInput(builder: StreamsBuilder,
                                         actionRequests: KStream[UUID, ActionRequest[A]],
                                         actionResponses: KStream[UUID, ActionResponse])

  val expectedTopicList = TopicTypes.ActionTopic.requestUnprocessed :: TopicTypes.ActionTopic.all

  private val actionTopicConfig = TopicConfigBuilder.buildTopics(expectedTopicList, Map.empty)(topicBuildFn)
  private val actionSpec        = ActionProcessorSpec[A](actionSerdes)

  type AsyncTransformer = AsyncTransformerInput => Properties => AsyncPipe

  private var transformers: List[AsyncTransformer] = List.empty
  private var expectedTopics                       = TopicCreation.allTopics(actionTopicConfig)

  private var closeHandlers: List[() => Unit] = List.empty

  def addAsync[I, K, O, R](spec: AsyncSpec[A, I, K, O, R])(
      implicit executionContext: ExecutionContext): AsyncApp[A] = {
    val ctx = AsyncContext(actionSpec, actionTopicConfig.namer, spec)
    val transformer: AsyncTransformer = input => {

      // join the action request with corresponding prior command responses
      AsyncStream.addSubTopology[A, I, K, O, R](ctx, input.actionRequests, input.actionResponses)

      new AsyncPipe { override def close(): Unit = {} }
      AsyncTransform.async(ctx)
    }
    transformers = transformer :: transformers
    expectedTopics = expectedTopics ++ spec.outputSpec.fold(List.empty[TopicCreation])(_.topicCreation)
    this
  }

  def addCloseHandler(handler: => Unit): Unit = {
    closeHandlers = (() => handler) :: closeHandlers
  }

  def run(appConfig: StreamAppConfig): Unit = {
    val config = StreamAppUtils.getConfig(appConfig)

    StreamAppUtils
      .addMissingTopics(AdminClient.create(config))(expectedTopics.distinct)
      .all()
      .get(30L, TimeUnit.SECONDS)

    val builder = new StreamsBuilder()
    val actionRequests: KStream[UUID, messages.ActionRequest[A]] =
      ActionConsumer.actionRequestStream(actionSpec, actionTopicConfig.namer, builder)
    val actionResponses: KStream[UUID, messages.ActionResponse] =
      ActionConsumer.actionResponseStream(actionSpec, actionTopicConfig.namer, builder)

    val commandInput          = AsyncTransformerInput(builder, actionRequests, actionResponses)
    val pipes: Seq[AsyncPipe] = transformers.map(x => x(commandInput)(config))

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
