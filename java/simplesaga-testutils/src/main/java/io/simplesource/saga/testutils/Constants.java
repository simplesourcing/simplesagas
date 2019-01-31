package io.simplesource.saga.testutils;

public class Constants {
    // sagas
    public static final String sagaTopicPrefix = "saga_coordinator_";
    public static final String sagaBaseName    = "saga";

    // action processors
    public static final String actionTopicPrefix  = "saga_action_processor_";
    public static final String sagaActionBaseName = "saga_action";

    // commands (simple sourcing)
    public static final String commandTopicPrefix = "saga_command_";
    // user aggregate
    public static final String userAggregateName = "user";
    public static final String userActionType    = "sourcing_action_user";
    // account aggregate
    public static final String accountAggregateName = "account";
    public static final String accountActionType    = "sourcing_action_account";

    public static final String kafkaBootstrap = "localhost:9092";

    public static final int partitions    = 6;
    public static final int replication   = 1;
    public static final int retentionDays = 7;
}
