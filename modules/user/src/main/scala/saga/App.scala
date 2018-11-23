package saga

import io.circe.Json
import io.simplesource.kafka.spec.WindowSpec
import model.specs.{ActionProcessorSpec, SagaSpec}
import shared.TopicUtils
import shared.serdes.JsonSerdes
import shared.utils.StreamAppConfig

object App {
  def main(args: Array[String]): Unit = {
    startSagaCoordinator()
  }

  def startSagaCoordinator(): Unit = {
    val sagaSpec = SagaSpec(JsonSerdes.sagaSerdes[Json], new WindowSpec(3600L))
    SagaApp[Json](sagaSpec, TopicUtils.buildSteps(constants.sagaTopicPrefix, constants.sagaBaseName))
      .addActionProcessor(actionProcessorSpec,
                          TopicUtils.buildSteps(constants.actionTopicPrefix, constants.sagaActionBaseName))
      .run(StreamAppConfig(appId = "saga-coordinator-1", bootstrapServers = constants.kafkaBootstrap))
  }

  lazy val actionProcessorSpec: ActionProcessorSpec[Json] =
    ActionProcessorSpec[Json](JsonSerdes.actionSerdes[Json])
}
