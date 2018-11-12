package action.sourcing
import model.specs.ActionProcessorSpec

/**
  * @param actionSpec
  * @param commandSpec
  * @tparam A - common representation form for all action commands (typically Json / GenericRecord for Avro)
  * @tparam I - intermediate decoded input type (that can easily be converted to both K and C)
  * @tparam K - aggregate key
  * @tparam C - simple sourcing command type
  */
final case class SourcingContext[A, I, K, C](actionSpec: ActionProcessorSpec[A],
                                             commandSpec: CommandSpec[A, I, K, C]) {
  val cSerdes = commandSpec.serdes
  val aSerdes = actionSpec.serdes
}
