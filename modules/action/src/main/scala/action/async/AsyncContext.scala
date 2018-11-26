package action.async
import model.specs.ActionProcessorSpec
import shared.topics.TopicNamer
/**
  * @tparam A - common representation form for all action commands (typically Json / or GenericRecord for Avro)
  * @tparam I - input to async function
  * @tparam K - key for the output topic
  * @tparam O - output returned by async function
  * @tparam R - final result type that ends up in output topic
  */
final case class AsyncContext[A, I, K, O, R](actionSpec: ActionProcessorSpec[A],
                                             actionTopicNamer: TopicNamer,
                                             asyncSpec: AsyncSpec[A, I, K, O, R]) {
  val outputSerdes = asyncSpec.outputSpec.map(_.serdes)
  val actionSerdes = actionSpec.serdes
}
