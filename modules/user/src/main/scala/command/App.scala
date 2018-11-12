package command
import java.util.UUID

import command.handlers.{AccountHandlers, UserHandlers}
import command.model.auction.{Account, AccountCommand, AccountEvent}
import command.model.user.{User, UserCommand, UserEvent}
import io.simplesource.kafka.dsl.{AggregateBuilder, AggregateSetBuilder, InvalidSequenceStrategy}
import io.simplesource.kafka.internal.streams.PrefixResourceNamingStrategy
import shared.serdes.JsonSerdes
import io.circe.generic.auto._

object App {
  def main(args: Array[String]): Unit = {
    startCommandProcessor()
  }

  def startCommandProcessor(): Unit = {
    new AggregateSetBuilder()
      .withKafkaConfig(
        builder =>
          builder
            .withKafkaApplicationId("ScalaUserRunner")
            .withKafkaBootstrap("localhost:9092")
            .withApplicationServer("localhost:1234")
            .build)
      .addAggregate(
        AggregateBuilder
          .newBuilder[UUID, UserCommand, UserEvent, Option[User]]()
          .withAggregator((a, e) => UserHandlers.aggregator(a)(e))
          .withCommandHandler((k, a, c) => UserHandlers.commandHandler(k, a)(c))
          .withSerdes(JsonSerdes.aggregateSerdes[UUID, UserCommand, UserEvent, Option[User]])
          .withResourceNamingStrategy(new PrefixResourceNamingStrategy(constants.commandTopicPrefix))
          .withName(constants.userAggregateName)
          .withInitialValue(_ => None)
          .withInvalidSequenceStrategy(InvalidSequenceStrategy.Strict)
          .build())
      .addAggregate(
        AggregateBuilder
          .newBuilder[UUID, AccountCommand, AccountEvent, Option[Account]]()
          .withAggregator((a, e) => AccountHandlers.aggregator(a)(e))
          .withCommandHandler((k, a, c) => AccountHandlers.commandHandler(k, a)(c))
          .withSerdes(JsonSerdes.aggregateSerdes[UUID, AccountCommand, AccountEvent, Option[Account]])
          .withResourceNamingStrategy(new PrefixResourceNamingStrategy(constants.commandTopicPrefix))
          .withName(constants.accountAggregateName)
          .withInitialValue(_ => None)
          .withInvalidSequenceStrategy(InvalidSequenceStrategy.Strict)
          .build())
      .build
    ()
  }
}
