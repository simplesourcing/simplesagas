package saga
import java.util.concurrent.{Executors, ScheduledExecutorService}

import builder.SagaClientBuilder
import io.simplesource.kafka.dsl.KafkaConfig
import io.simplesource.kafka.internal.util.NamedThreadFactory
import model.api.SagaAPI

final class SagaClient private (kafkaConfig: KafkaConfig, schedulerWithDefault: ScheduledExecutorService) {

  def createSagaApi[A](sagaBuildSteps: SagaClientBuilder.BuildSteps[A]): SagaAPI[A] = {
    val builder = new SagaClientBuilder[A](kafkaConfig, schedulerWithDefault)
    sagaBuildSteps.apply(builder)
    builder.build()
  }
}

object SagaClient {
  def apply(kafkaConfigSteps: KafkaConfig.Builder => KafkaConfig.Builder,
            scheduler: Option[ScheduledExecutorService] = None): SagaClient = {
    val schedulerWithDefault: ScheduledExecutorService =
      scheduler.getOrElse(
        Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("SagaClient-scheduler")))

    val kafkaConfig: KafkaConfig = {
      val builder = new KafkaConfig.Builder()
      kafkaConfigSteps(builder)
      builder.build(true)
    }
    new SagaClient(kafkaConfig, schedulerWithDefault)
  }
}
