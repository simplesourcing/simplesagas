package saga.app
import model.serdes
import model.specs.{ActionProcessorSpec, SagaSpec}
import model.topics.TopicNamer

final case class SagaContext[A](sSpec: SagaSpec[A], aSpec: ActionProcessorSpec[A]) {
  val sSerdes: serdes.SagaSerdes[A]   = sSpec.serdes
  val aSerdes: serdes.ActionSerdes[A] = aSpec.serdes
  val sagaTopicNamer: TopicNamer      = sSpec.topicConfig.namer
  val actionTopicNamer: TopicNamer    = aSpec.topicConfig.namer
}
