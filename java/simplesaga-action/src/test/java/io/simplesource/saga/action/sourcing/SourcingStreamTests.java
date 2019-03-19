package io.simplesource.saga.action.sourcing;

import io.simplesource.data.Result;
import io.simplesource.data.Sequence;
import io.simplesource.kafka.api.CommandSerdes;
import io.simplesource.kafka.model.CommandRequest;
import io.simplesource.kafka.model.CommandResponse;
import io.simplesource.kafka.serialization.avro.AvroCommandSerdes;
import io.simplesource.saga.avro.avro.generated.test.AccountCommand;
import io.simplesource.saga.avro.avro.generated.test.AccountId;
import io.simplesource.saga.avro.avro.generated.test.CreateAccount;
import io.simplesource.saga.model.action.ActionCommand;
import io.simplesource.saga.model.messages.ActionRequest;
import io.simplesource.saga.model.messages.ActionResponse;
import io.simplesource.saga.model.serdes.ActionSerdes;
import io.simplesource.saga.serialization.avro.AvroSerdes;
import io.simplesource.saga.shared.topics.TopicNamer;
import io.simplesource.saga.shared.topics.TopicTypes;
import io.simplesource.saga.shared.utils.StreamAppConfig;
import io.simplesource.saga.testutils.*;
import lombok.Value;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.streams.Topology;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class SourcingStreamTests {

    private static String SCHEMA_URL = "http://localhost:8081/";
    private static String TOPIC_BASE_NAME = "topic-base-name";
    private static String ACCOUNT_ID = "account id";

    @Value
    private static class AccountCreateContext {
        final TestContext testContext;

        // serdes
        final ActionSerdes<SpecificRecord> actionSerdes = AvroSerdes.Specific.actionSerdes(SCHEMA_URL, true);
        final CommandSerdes<AccountId, AccountCommand> commandSerdes = AvroCommandSerdes.of(SCHEMA_URL, true);

        // publishers
        final RecordPublisher<UUID, ActionRequest<SpecificRecord>> actionRequestPublisher;
        final RecordPublisher<AccountId, CommandResponse> commandResponsePublisher;
        final RecordPublisher<UUID, ActionResponse> actionResponsePublisher;

        // verifiers
        final RecordVerifier<AccountId, CommandRequest<AccountId, AccountCommand>> commandRequestVerifier;
        final RecordVerifier<UUID, ActionResponse> actionResponseVerifier;

        AccountCreateContext() {
            CommandSpec<SpecificRecord, AccountCommand, AccountId, AccountCommand> commandSpec = new CommandSpec<>(
                    Constants.accountActionType,
                    a -> Result.success((AccountCommand) a),
                    c -> c,
                    AccountCommand::getId,
                    commandSerdes,
                    Constants.accountAggregateName,
                    2000);

            SourcingApp<SpecificRecord> sourcingApp = new SourcingApp<>(actionSerdes,
                    TopicUtils.buildSteps(Constants.actionTopicPrefix, TOPIC_BASE_NAME));
            sourcingApp.addCommand(commandSpec, TopicUtils.buildSteps(Constants.commandTopicPrefix, Constants.accountAggregateName));
            Topology topology = sourcingApp.buildTopology(new StreamAppConfig("app-id", "http://localhost:9092"));
            testContext = TestContextBuilder.of(topology).build();

            // get actionRequestPublisher
            actionRequestPublisher = testContext.publisher(
                    TopicNamer.forPrefix(Constants.actionTopicPrefix, TOPIC_BASE_NAME)
                            .apply(TopicTypes.ActionTopic.request),
                    actionSerdes.uuid(),
                    actionSerdes.request());

            commandResponsePublisher = testContext.publisher(
                    TopicNamer.forPrefix(Constants.commandTopicPrefix, Constants.accountAggregateName)
                            .apply(TopicTypes.CommandTopic.response),
                    commandSerdes.aggregateKey(),
                    commandSerdes.commandResponse());

            actionResponsePublisher = testContext.publisher(
                    TopicNamer.forPrefix(Constants.actionTopicPrefix, TOPIC_BASE_NAME)
                            .apply(TopicTypes.ActionTopic.response),
                    actionSerdes.uuid(),
                    actionSerdes.response());

            // get commandRequestVerifier
            commandRequestVerifier = testContext.verifier(
                    TopicNamer.forPrefix(Constants.commandTopicPrefix, Constants.accountAggregateName)
                            .apply(TopicTypes.CommandTopic.request),
                    commandSerdes.aggregateKey(),
                    commandSerdes.commandRequest());

            actionResponseVerifier = testContext.verifier(
                    TopicNamer.forPrefix(Constants.actionTopicPrefix, TOPIC_BASE_NAME)
                            .apply(TopicTypes.ActionTopic.response),
                    actionSerdes.uuid(),
                    actionSerdes.response());

        }
    }

    private static ActionRequest<SpecificRecord> createRequest(AccountCommand accountCommand, UUID commandId) {
        ActionCommand<SpecificRecord> actionCommand = new ActionCommand<>(commandId, accountCommand);
        return ActionRequest.<SpecificRecord>builder()
                .sagaId(UUID.randomUUID())
                .actionId(UUID.randomUUID())
                .actionCommand(actionCommand)
                .actionType(Constants.accountActionType)
                .build();
    }

    @Test
    void actionRequestGeneratesCommandRequest() {

        AccountCreateContext acc = new AccountCreateContext();

        CreateAccount createAccount = new CreateAccount(ACCOUNT_ID, "user name");
        AccountCommand accountCommand = new AccountCommand(new AccountId(createAccount.getId()), createAccount);

        ActionRequest<SpecificRecord> actionRequest = createRequest(accountCommand, UUID.randomUUID());

        acc.actionRequestPublisher().publish(actionRequest.sagaId, actionRequest);

        acc.commandRequestVerifier().verifySingle((accountId, commandRequest) -> {
            assertThat(accountId.getId()).isEqualTo(ACCOUNT_ID);
            assertThat(commandRequest.command()).isEqualToComparingFieldByField(accountCommand);
        });
        acc.commandRequestVerifier().verifyNoRecords();
    }

    @Test
    void commandResponseGeneratesActionResponse() {

        AccountCreateContext acc = new AccountCreateContext();

        CreateAccount createAccount = new CreateAccount(ACCOUNT_ID, "user name");
        AccountCommand accountCommand = new AccountCommand(new AccountId(createAccount.getId()), createAccount);

        UUID commandId = UUID.randomUUID();
        ActionRequest<SpecificRecord> actionRequest = createRequest(accountCommand, commandId);

        acc.actionRequestPublisher().publish(actionRequest.sagaId, actionRequest);
        acc.commandRequestVerifier().drainAll();

        CommandResponse commandResponse = new CommandResponse(commandId, Sequence.position(201L), Result.success(Sequence.position(202L)));
        acc.commandResponsePublisher().publish(new AccountId(createAccount.getId()), commandResponse);

        acc.actionResponseVerifier().verifySingle((sagaId, actionResponse) -> {
            assertThat(sagaId).isEqualTo(actionRequest.sagaId);
            assertThat(actionResponse.actionId).isEqualTo(actionRequest.actionId);
            assertThat(actionResponse.sagaId).isEqualTo(actionRequest.sagaId);
            assertThat(actionResponse.result.isSuccess()).isEqualTo(true);
        });
        acc.actionResponseVerifier().verifyNoRecords();
    }

    @Test
    void actionIndempotence() {

        AccountCreateContext acc = new AccountCreateContext();

        CreateAccount createAccount = new CreateAccount(ACCOUNT_ID, "user name");
        AccountCommand accountCommand = new AccountCommand(new AccountId(createAccount.getId()), createAccount);

        UUID commandId = UUID.randomUUID();
        ActionRequest<SpecificRecord> actionRequest = createRequest(accountCommand, commandId);

        acc.actionRequestPublisher().publish(actionRequest.sagaId, actionRequest);
        acc.commandRequestVerifier().drainAll();

        CommandResponse commandResponse = new CommandResponse(commandId, Sequence.position(201L), Result.success(Sequence.position(202L)));
        acc.commandResponsePublisher().publish(new AccountId(createAccount.getId()), commandResponse);

        acc.actionResponseVerifier().verifySingle((sagaId, actionResponse) -> {});
        acc.actionResponseVerifier().verifyNoRecords();

        acc.actionRequestPublisher().publish(actionRequest.sagaId, actionRequest);
        // does not generate a command request
        acc.commandRequestVerifier().verifyNoRecords();
        // regenerates an action response

        acc.actionResponseVerifier().verifySingle((sagaId, actionResponse) -> {
            assertThat(sagaId).isEqualTo(actionRequest.sagaId);
            assertThat(actionResponse.actionId).isEqualTo(actionRequest.actionId);
            assertThat(actionResponse.result.isSuccess()).isEqualTo(true);
        });
        acc.actionResponseVerifier().verifyNoRecords();
    }
}
