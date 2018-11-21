package action.common

import java.util.UUID

import model.messages.{ActionRequest, ActionResponse}
import model.specs.ActionProcessorSpec
import model.topics
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.kstream.{Consumed, KStream}
import org.slf4j.LoggerFactory

object ActionConsumer {

  def actionRequestStream[A](spec: ActionProcessorSpec[A],
                             builder: StreamsBuilder): KStream[UUID, ActionRequest[A]] =
    builder.stream[UUID, ActionRequest[A]](
      spec.topicConfig.namer(topics.ActionTopic.request),
      Consumed.`with`(spec.serdes.uuid, spec.serdes.request)
    )

  def actionResponseStream[A](spec: ActionProcessorSpec[A],
                              builder: StreamsBuilder): KStream[UUID, ActionResponse] =
    builder.stream[UUID, ActionResponse](
      spec.topicConfig.namer(topics.ActionTopic.response),
      Consumed.`with`(spec.serdes.uuid, spec.serdes.response)
    )
}
