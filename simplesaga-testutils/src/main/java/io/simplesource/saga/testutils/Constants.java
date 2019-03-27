package io.simplesource.saga.testutils;

public class Constants {
    // sagas
    public static final String SAGA_TOPIC_PREFIX = "saga_coordinator-";

    // action processors
    public static final String ACTION_TOPIC_PREFIX = "saga_action_processor-";
    public static final String SAGA_ACTION_TYPE = "saga_action_type";

    // commands (simple sourcing)
    public static final String COMMAND_TOPIC_PREFIX = "saga_command-";

    // account aggregate
    public static final String ACCOUNT_AGGREGATE_NAME = "account";
    public static final String ACCOUNT_ACTION_TYPE = "sourcing_action_account";

    public static final String ASYNC_TEST_ACTION_TYPE = "async_action_test";

    static final int PARTITIONS = 6;
    static final int REPLICATION = 1;
    static final int RETENTION_DAYS = 7;
}
