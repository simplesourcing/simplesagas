package io.simplesource.saga.user.saga

import io.circe.Json
import io.simplesource.kafka.spec.WindowSpec
import io.simplesource.saga.model.specs.{ActionProcessorSpec, SagaSpec}
import io.simplesource.saga.saga.SagaApp
import io.simplesource.saga.shared.utils.StreamAppConfig
import io.simplesource.saga.user.shared.TopicUtils
import io.simplesource.saga.scala.serdes.JsonSerdes

object App {
  def main(args: Array[String]): Unit = {
    startSagaCoordinator()
  }

  def startSagaCoordinator(): Unit = {
    val sagaSpec =
      new SagaSpec(JsonSerdes.sagaSerdes[Json], new WindowSpec(3600L))
    new SagaApp[Json](sagaSpec, TopicUtils.buildSteps(constants.sagaTopicPrefix, constants.sagaBaseName))
      .addActionProcessor(actionProcessorSpec,
                          TopicUtils.buildSteps(constants.actionTopicPrefix, constants.sagaActionBaseName))
      .run(new StreamAppConfig("saga-coordinator-1", constants.kafkaBootstrap))
  }

  lazy val actionProcessorSpec: ActionProcessorSpec[Json] =
    new ActionProcessorSpec[Json](JsonSerdes.actionSerdes[Json])
}
