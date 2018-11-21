package saga

import io.circe.Json
import model.specs.ActionProcessorSpec
import topics.serdes.JsonSerdes
import topics.utils.StreamAppConfig

object App {
  def main(args: Array[String]): Unit = {
    startSagaCoordinator()
  }

  def startSagaCoordinator(): Unit = {
    SagaApp[Json](JsonSerdes.sagaSerdes[Json],
                  topics.buildSteps(constants.sagaTopicPrefix, constants.sagaBaseName))
      .addActionProcessor(actionProcessorSpec,
                          topics.buildSteps(constants.actionTopicPrefix, constants.sagaActionBaseName))
      .run(StreamAppConfig(appId = "saga-coordinator-1", bootstrapServers = constants.kafkaBootstrap))
  }

  lazy val actionProcessorSpec: ActionProcessorSpec[Json] =
    ActionProcessorSpec[Json](JsonSerdes.actionSerdes[Json])
}
