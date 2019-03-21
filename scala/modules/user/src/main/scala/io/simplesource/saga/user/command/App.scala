package io.simplesource.saga.user.command
import java.util.UUID

import io.simplesource.saga.user.command.handlers.{AccountHandlers, UserHandlers}
import io.simplesource.saga.user.command.model.auction.{Account, AccountCommand, AccountEvent}
import io.simplesource.saga.user.command.model.user.{User, UserCommand, UserEvent}
import io.simplesource.kafka.dsl.{AggregateBuilder, EventSourcedApp, InvalidSequenceStrategy}
import io.simplesource.saga.scala.serdes.JsonSerdes
import io.circe.generic.auto._
import io.simplesource.kafka.util.PrefixResourceNamingStrategy

object App {
  def main(args: Array[String]): Unit = {
    startCommandProcessor()
  }

  def startCommandProcessor(): Unit = {
    new EventSourcedApp()
      .withKafkaConfig(
        builder =>
          builder
            .withKafkaApplicationId("ScalaUserRunner")
            .withKafkaBootstrap(constants.kafkaBootstrap)
            .build)
      .addAggregate(
        AggregateBuilder
          .newBuilder[UUID, UserCommand, UserEvent, Option[User]]()
          .withAggregator((a, e) => UserHandlers.aggregator(a)(e))
          .withCommandHandler((k, a, c) => UserHandlers.commandHandler(k, a)(c))
          .withSerdes(JsonSerdes
            .aggregateSerdes[UUID, UserCommand, UserEvent, Option[User]])
          .withResourceNamingStrategy(new PrefixResourceNamingStrategy(constants.commandTopicPrefix))
          .withName(constants.userAggregateName)
          .withInitialValue(_ => None)
          .withInvalidSequenceStrategy(InvalidSequenceStrategy.LastWriteWins) // TODO: re-enable sequence checking
          .withDefaultTopicSpec(constants.partitions, constants.replication, constants.retentionDays)
          .build())
      .addAggregate(
        AggregateBuilder
          .newBuilder[UUID, AccountCommand, AccountEvent, Option[Account]]()
          .withAggregator((a, e) => AccountHandlers.aggregator(a)(e))
          .withCommandHandler((k, a, c) => AccountHandlers.commandHandler(k, a)(c))
          .withSerdes(JsonSerdes
            .aggregateSerdes[UUID, AccountCommand, AccountEvent, Option[Account]])
          .withResourceNamingStrategy(new PrefixResourceNamingStrategy(constants.commandTopicPrefix))
          .withName(constants.accountAggregateName)
          .withInitialValue(_ => None)
          .withInvalidSequenceStrategy(InvalidSequenceStrategy.LastWriteWins) // TODO: re-enable sequence checking
          .withDefaultTopicSpec(constants.partitions, constants.replication, constants.retentionDays)
          .build())
      .start()
    ()
  }
}
