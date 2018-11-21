package action.common

import java.util.UUID

import model.messages.{ActionRequest, ActionResponse}
import model.specs.ActionProcessorSpec
import model.topics
import model.topics.TopicNamer
import org.apache.kafka.streams.kstream.{KStream, Produced}

object ActionProducer {

  def actionResponse[A](actionProcessorSpec: ActionProcessorSpec[A],
                        topicNamer: TopicNamer,
                        responses: KStream[UUID, ActionResponse]*): Unit = {
    responses.foreach(
      _.to(
        topicNamer(topics.ActionTopic.response),
        Produced.`with`(actionProcessorSpec.serdes.uuid, actionProcessorSpec.serdes.response)
      ))
  }

  def actionRequest[A](actionSpec: ActionProcessorSpec[A],
                       topicNamer: TopicNamer,
                       request: KStream[UUID, ActionRequest[A]],
                       unprocessed: Boolean): Unit = {
    request.to(topicNamer(topics.ActionTopic.requestUnprocessed),
               Produced.`with`(actionSpec.serdes.uuid, actionSpec.serdes.request))
  }
}
