package saga.app
import model.serdes
import model.specs.{ActionProcessorSpec, SagaSpec}
import model.topics.TopicNamer

final case class SagaContext[A](sSpec: SagaSpec[A],
                                aSpec: ActionProcessorSpec[A],
                                actionTopicNamer: TopicNamer) {
  val sSerdes: serdes.SagaSerdes[A]   = sSpec.serdes
  val aSerdes: serdes.ActionSerdes[A] = aSpec.serdes
  val sagaTopicNamer: TopicNamer      = sSpec.topicConfig.namer
}
