package action.sourcing

import io.simplesource.kafka.model.CommandResponse
import model.topics
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.kstream.{Consumed, KStream}

object CommandConsumer {

  def commandResponseStream[A, I, K, C](spec: CommandSpec[A, I, K, C],
                                        builder: StreamsBuilder): KStream[K, CommandResponse] = {
    val responseByAggregate = builder
      .stream[K, CommandResponse](spec.topicNamer(topics.CommandTopic.response),
                                  Consumed.`with`(spec.serdes.aggregateKey, spec.serdes.commandResponse()))
      .peek(SourcingStream.logValues[K, CommandResponse]("commandResponseStream"))
    responseByAggregate
  }
}
