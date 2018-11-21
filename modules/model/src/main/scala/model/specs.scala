package model
import model.serdes.{ActionSerdes, SagaSerdes}

object specs {
  final case class SagaSpec[A](serdes: SagaSerdes[A])
  final case class ActionProcessorSpec[A](serdes: ActionSerdes[A])
}
