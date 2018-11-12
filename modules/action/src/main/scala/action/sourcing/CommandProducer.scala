package action.sourcing

import io.simplesource.kafka.model.CommandRequest
import model.topics
import org.apache.kafka.streams.kstream.{KStream, Produced}

object CommandProducer {

  def commandRequest[A, I, K, C](cSpec: CommandSpec[A, I, K, C],
                                 commandRequestByAggregate: KStream[K, CommandRequest[K, C]]): Unit = {
    // publish to command request topic
    commandRequestByAggregate.to(
      cSpec.topicNamer(topics.CommandTopic.request),
      Produced.`with`(cSpec.serdes.aggregateKey, cSpec.serdes.request)
    )
  }
}
