package saga

import io.circe.Json
import io.simplesource.kafka.spec.WindowSpec
import io.simplesource.saga.model.specs.{ActionProcessorSpec, SagaSpec}
import io.simplesource.saga.shared.utils.StreamAppConfig
import shared.TopicUtils
import shared.serdes.JsonSerdes

object App {
  def main(args: Array[String]): Unit = {
    startSagaCoordinator()
  }

  def startSagaCoordinator(): Unit = {
    val sagaSpec = new SagaSpec(JsonSerdes.sagaSerdes[Json], new WindowSpec(3600L))
    new SagaApp[Json](sagaSpec, TopicUtils.buildStepsJ(constants.sagaTopicPrefix, constants.sagaBaseName))
      .addActionProcessor(actionProcessorSpec,
                          TopicUtils.buildStepsJ(constants.actionTopicPrefix, constants.sagaActionBaseName))
      .run(new StreamAppConfig("saga-coordinator-1", constants.kafkaBootstrap))
  }

  lazy val actionProcessorSpec: ActionProcessorSpec[Json] =
    new ActionProcessorSpec[Json](JsonSerdes.actionSerdes[Json])
}
