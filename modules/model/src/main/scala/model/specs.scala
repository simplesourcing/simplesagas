package model
import model.serdes.{ActionSerdes, SagaSerdes}
import model.topics.TopicConfig

object specs {
  final case class SagaSpec[A](serdes: SagaSerdes[A], topicConfig: TopicConfig)

  final case class ActionProcessorSpec[A](serdes: ActionSerdes[A])
}
