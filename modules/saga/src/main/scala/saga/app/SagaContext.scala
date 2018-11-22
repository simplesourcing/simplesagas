package saga.app
import model.serdes
import model.specs.{ActionProcessorSpec, SagaSpec}
import shared.topics.TopicNamer
final case class SagaContext[A](sSpec: SagaSpec[A],
                                aSpec: ActionProcessorSpec[A],
                                sagaTopicNamer: TopicNamer,
                                actionTopicNamer: TopicNamer) {
  val sSerdes: serdes.SagaSerdes[A]   = sSpec.serdes
  val aSerdes: serdes.ActionSerdes[A] = aSpec.serdes
}
