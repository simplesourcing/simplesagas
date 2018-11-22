package saga

import io.circe.Json
import model.specs.ActionProcessorSpec
import shared.serdes.JsonSerdes
import shared.utils.StreamAppConfig

object App {
  def main(args: Array[String]): Unit = {
    startSagaCoordinator()
  }

  def startSagaCoordinator(): Unit = {
    SagaApp[Json](JsonSerdes.sagaSerdes[Json],
                  shared.buildSteps(constants.sagaTopicPrefix, constants.sagaBaseName))
      .addActionProcessor(actionProcessorSpec,
                          shared.buildSteps(constants.actionTopicPrefix, constants.sagaActionBaseName))
      .run(StreamAppConfig(appId = "saga-coordinator-1", bootstrapServers = constants.kafkaBootstrap))
  }

  lazy val actionProcessorSpec: ActionProcessorSpec[Json] =
    ActionProcessorSpec[Json](JsonSerdes.actionSerdes[Json])
}
