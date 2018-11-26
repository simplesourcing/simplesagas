package io.simplesource.action.sourcing

import io.simplesource.kafka.model.CommandRequest
import org.apache.kafka.streams.kstream.{KStream, Produced}
import shared.topics.{TopicNamer, TopicTypes}

object CommandProducer {

  def commandRequest[A, I, K, C](cSpec: CommandSpec[A, I, K, C],
                                 commandTopicNamer: TopicNamer,
                                 commandRequestByAggregate: KStream[K, CommandRequest[K, C]]): Unit = {
    // publish to command request topic
    commandRequestByAggregate.to(
      commandTopicNamer(TopicTypes.CommandTopic.request),
      Produced.`with`(cSpec.serdes.aggregateKey, cSpec.serdes.commandRequest())
    )
  }
}
