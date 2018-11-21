package saga

import io.circe.Json
import io.simplesource.kafka.util.PrefixResourceNamingStrategy
import model.specs.{ActionProcessorSpec, SagaSpec}
import model.topics
import shared.utils.{StreamAppConfig, TopicConfigurer}
import shared.serdes.JsonSerdes

object App {
  def main(args: Array[String]): Unit = {
    startSagaCoordinator()
  }

  def startSagaCoordinator(): Unit = {
    SagaApp[Json](sagaSpec)
      .addActionProcessor(actionProcessorSpec)
      .run(StreamAppConfig(appId = "saga-coordinator-1", bootstrapServers = constants.kafkaBootstrap))
  }

  lazy val sagaSpec: SagaSpec[Json] = SagaSpec[Json](
    JsonSerdes.sagaSerdes[Json],
    TopicConfigurer.forStrategy(new PrefixResourceNamingStrategy(constants.sagaTopicPrefix),
                                constants.sagaBaseName,
                                topics.SagaTopic.all)
  )

  lazy val actionProcessorSpec: ActionProcessorSpec[Json] = ActionProcessorSpec[Json](
    JsonSerdes.actionSerdes[Json],
    TopicConfigurer.forStrategy(new PrefixResourceNamingStrategy(constants.actionTopicPrefix),
                                constants.sagaActionBaseName,
                                topics.ActionTopic.all)
  )
}
