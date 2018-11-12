package model
import model.serdes.{ActionSerdes, SagaSerdes}
import model.topics.TopicNamer

object specs {
  final case class SagaSpec[A](serdes: SagaSerdes[A], topicNamer: TopicNamer[String])

  final case class ActionProcessorSpec[A](serdes: ActionSerdes[A], topicNamer: TopicNamer[String])
}
