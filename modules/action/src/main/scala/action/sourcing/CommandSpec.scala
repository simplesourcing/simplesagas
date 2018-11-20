package action.sourcing
import io.simplesource.kafka.api.CommandSerdes
import model.topics.TopicConfig

/**
  * @param actionType
  * @param decode
  * @param commandMapper
  * @param keyMapper
  * @param serdes
  * @param topicConfig
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
                                         topicConfig: TopicConfig,
                                         aggregateName: String,
                                         timeOutMillis: Long)
