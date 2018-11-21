package action.sourcing

import io.simplesource.kafka.model.CommandResponse
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.kstream.{Consumed, KStream}
import topics.topics.{TopicNamer, TopicTypes}

object CommandConsumer {

  def commandResponseStream[A, I, K, C](spec: CommandSpec[A, I, K, C],
                                        commandTopicNamer: TopicNamer,
                                        builder: StreamsBuilder): KStream[K, CommandResponse] = {
    val responseByAggregate = builder
      .stream[K, CommandResponse](commandTopicNamer(TopicTypes.CommandTopic.response),
                                  Consumed.`with`(spec.serdes.aggregateKey, spec.serdes.commandResponse()))
      .peek(SourcingStream.logValues[K, CommandResponse]("commandResponseStream"))
    responseByAggregate
  }
}
