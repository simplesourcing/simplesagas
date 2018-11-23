package model
import io.simplesource.kafka.spec.WindowSpec
import model.serdes.{ActionSerdes, SagaSerdes}

object specs {
  final case class SagaSpec[A](serdes: SagaSerdes[A], responseWindow: WindowSpec)
  final case class ActionProcessorSpec[A](serdes: ActionSerdes[A])
}
