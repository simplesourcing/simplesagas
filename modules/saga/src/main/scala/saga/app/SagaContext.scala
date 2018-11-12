package saga.app
import model.specs.{ActionProcessorSpec, SagaSpec}

final case class SagaContext[A](sSpec: SagaSpec[A], aSpec: ActionProcessorSpec[A]) {
  val sSerdes = sSpec.serdes
  val aSerdes = aSpec.serdes
}
