package object constants {
  // sagas
  val sagaTopicPrefix = "saga_coordinator_"
  val sagaBaseName    = "io/simplesource/saga/user/saga"

  // action processors
  val actionTopicPrefix  = "saga_action_processor_"
  val sagaActionBaseName = "saga_action"

  // commands (simple sourcing)
  val commandTopicPrefix = "saga_command_"
  // user aggregate
  val userAggregateName = "user"
  val userActionType    = "sourcing_action_user"
  // account aggregate
  val accountAggregateName = "account"
  val accountActionType    = "sourcing_action_account"

  val kafkaBootstrap = "localhost:9092"

  val partitions    = 6
  val replication   = 1
  val retentionDays = 7
}
