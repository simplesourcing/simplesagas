package action.sourcing

import io.simplesource.kafka.model.CommandResponse
import model.topics
import model.topics.TopicNamer
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.kstream.{Consumed, KStream}

object CommandConsumer {

  def commandResponseStream[A, I, K, C](spec: CommandSpec[A, I, K, C],
                                        commandTopicNamer: TopicNamer,
                                        builder: StreamsBuilder): KStream[K, CommandResponse] = {
    val responseByAggregate = builder
      .stream[K, CommandResponse](commandTopicNamer(topics.CommandTopic.response),
                                  Consumed.`with`(spec.serdes.aggregateKey, spec.serdes.commandResponse()))
      .peek(SourcingStream.logValues[K, CommandResponse]("commandResponseStream"))
    responseByAggregate
  }
}
