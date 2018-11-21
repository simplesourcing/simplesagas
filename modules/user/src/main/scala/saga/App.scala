package saga

import io.circe.Json
import io.simplesource.kafka.util.PrefixResourceNamingStrategy
import model.specs.{ActionProcessorSpec, SagaSpec}
import model.topics
import shared.utils.{StreamAppConfig, TopicConfigurer, TopicNamer}
import shared.serdes.JsonSerdes

object App {
  def main(args: Array[String]): Unit = {
    startSagaCoordinator()
  }

  def startSagaCoordinator(): Unit = {
    SagaApp[Json](JsonSerdes.sagaSerdes[Json],
                  _.withTopicNamer(TopicNamer.forPrefix(constants.sagaTopicPrefix, constants.sagaBaseName)))
      .addActionProcessor(
        actionProcessorSpec,
        _.withTopicNamer(TopicNamer.forPrefix(constants.actionTopicPrefix, constants.sagaActionBaseName)))
      .run(StreamAppConfig(appId = "saga-coordinator-1", bootstrapServers = constants.kafkaBootstrap))
  }

  lazy val sagaSpec: SagaSpec[Json] = SagaSpec[Json](
    JsonSerdes.sagaSerdes[Json],
    TopicConfigurer.forStrategy(new PrefixResourceNamingStrategy(constants.sagaTopicPrefix),
                                constants.sagaBaseName,
                                topics.SagaTopic.all)
  )

  lazy val actionProcessorSpec: ActionProcessorSpec[Json] =
    ActionProcessorSpec[Json](JsonSerdes.actionSerdes[Json])
}
