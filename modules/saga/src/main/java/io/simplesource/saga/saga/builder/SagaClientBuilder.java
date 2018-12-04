//package io.simplesource.saga.saga.builder
//
//import java.util.concurrent.ScheduledExecutorService
//
//import io.simplesource.kafka.dsl.KafkaConfig
//import io.simplesource.kafka.spec.WindowSpec
//import model.api.SagaAPI
//import model.serdes.SagaSerdes
//import model.specs.SagaSpec
//import saga.api.KafkaSagaAPI
//import shared.topics.{TopicConfig, TopicConfigBuilder, TopicTypes}
//
//final class SagaClientBuilder[A](kafkaConfig: KafkaConfig, scheduler: ScheduledExecutorService) {
//
//  var serdes: Option[SagaSerdes[A]]    = None
//  var topicConfig: Option[TopicConfig] = None
//  var clientId: Option[String]         = None
//  var windowSpec: WindowSpec           = new WindowSpec(3600L)
//
//  def withSerdes(serdes: SagaSerdes[A]): SagaClientBuilder[A] = {
//    this.serdes = Some(serdes)
//    this
//  }
//
//  def withResponseWindow(windowSpec: WindowSpec): SagaClientBuilder[A] = {
//    this.windowSpec = windowSpec
//    this
//  }
//
//  def withTopicConfig(topicBuildFn: TopicConfigBuilder.BuildSteps): SagaClientBuilder[A] = {
//    val tcb = TopicConfigBuilder(TopicTypes.SagaTopic.client, Map.empty)
//    topicBuildFn(tcb)
//    val topicConfig = tcb.build()
//    this.topicConfig = Some(topicConfig)
//    this
//  }
//
//  def withClientId(clientId: String): SagaClientBuilder[A] = {
//    this.clientId = Some(clientId)
//    this
//  }
//
//  def build(): SagaAPI[A] = {
//    assert(serdes.isDefined, "Serdes have not been defined")
//    assert(topicConfig.isDefined, "TopicConfig has not been defined")
//    assert(clientId.isDefined, "ClientId has not been defined")
//    val sagaSpec = SagaSpec(serdes.get, windowSpec)
//    new KafkaSagaAPI[A](sagaSpec, kafkaConfig, topicConfig.get, clientId.get, scheduler)
//  }
//}
//
//object SagaClientBuilder {
//  type BuildSteps[A] = SagaClientBuilder[A] => SagaClientBuilder[A]
//}
