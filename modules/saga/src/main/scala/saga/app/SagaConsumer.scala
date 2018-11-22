package saga.app
import java.util.UUID

import model.messages.{ActionResponse, SagaRequest, SagaStateTransition}
import model.saga.Saga
import model.specs.{ActionProcessorSpec, SagaSpec}
import shared.topics.TopicTypes.{ActionTopic, SagaTopic}
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.kstream.{Consumed, KStream}
import org.slf4j.LoggerFactory
import shared.topics.TopicNamer
object SagaConsumer {
  private val logger = LoggerFactory.getLogger("SagaConsumer")

  def sagaRequest[A](spec: SagaSpec[A],
                     sagaTopicNamer: TopicNamer,
                     builder: StreamsBuilder): KStream[UUID, SagaRequest[A]] =
    builder.stream[UUID, SagaRequest[A]](sagaTopicNamer(SagaTopic.request),
                                         Consumed.`with`(spec.serdes.uuid, spec.serdes.request))

  def stateTransition[A](spec: SagaSpec[A],
                         sagaTopicNamer: TopicNamer,
                         builder: StreamsBuilder): KStream[UUID, SagaStateTransition[A]] =
    builder
      .stream[UUID, SagaStateTransition[A]](sagaTopicNamer(SagaTopic.stateTransition),
                                            Consumed.`with`(spec.serdes.uuid, spec.serdes.transition))

  def state[A](spec: SagaSpec[A],
               sagaTopicNamer: TopicNamer,
               builder: StreamsBuilder): KStream[UUID, Saga[A]] = {
    builder
      .stream[UUID, Saga[A]](sagaTopicNamer(SagaTopic.state),
                             Consumed.`with`(spec.serdes.uuid, spec.serdes.state))
      .peek((k, state) =>
        logger.info(
          s"sagaState: ${k.toString.take(6)}=${state.status}=>${state.sequence}-${state.actions.values
            .map(v => (v.actionId.toString.take(6), v.status))
            .mkString("-")}"))
  }

  def actionResponse[A](actionSpec: ActionProcessorSpec[A],
                        topicNamer: TopicNamer,
                        builder: StreamsBuilder): KStream[UUID, ActionResponse] =
    builder.stream[UUID, ActionResponse](topicNamer(ActionTopic.response),
                                         Consumed.`with`(actionSpec.serdes.uuid, actionSpec.serdes.response))
}
