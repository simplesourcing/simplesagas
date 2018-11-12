package action.sourcing
import model.serdes.CommandSerdes
import model.topics.TopicNamer

/**
  * @param actionType
  * @param decode
  * @param commandMapper
  * @param keyMapper
  * @param serdes
  * @param topicNamer
  * @param aggregateName
  * @param timeOutMillis
  * @tparam A - common representation form for all action commands (typically Json / GenericRecord for Avro)
  * @tparam I - intermediate decoded input type (that can easily be converted both K and C)
  * @tparam K - aggregate key
  * @tparam C - simple sourcing command type
  */
final case class CommandSpec[A, I, K, C](actionType: String,
                                         decode: A => Either[Throwable, I],
                                         commandMapper: I => C,
                                         keyMapper: I => K,
                                         serdes: CommandSerdes[K, C],
                                         topicNamer: TopicNamer[String],
                                         aggregateName: String,
                                         timeOutMillis: Long)
