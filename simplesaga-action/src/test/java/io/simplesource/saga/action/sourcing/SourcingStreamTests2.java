package io.simplesource.saga.action.sourcing;

import io.simplesource.api.CommandId;
import io.simplesource.data.Result;
import io.simplesource.data.Sequence;
import io.simplesource.kafka.api.CommandSerdes;
import io.simplesource.kafka.model.CommandRequest;
import io.simplesource.kafka.model.CommandResponse;
import io.simplesource.kafka.serialization.avro.AvroCommandSerdes;
import io.simplesource.saga.action.common.StreamApp;
import io.simplesource.saga.avro.avro.generated.test.*;
import io.simplesource.saga.model.action.ActionCommand;
import io.simplesource.saga.model.action.ActionId;
import io.simplesource.saga.model.messages.ActionRequest;
import io.simplesource.saga.model.messages.ActionResponse;
import io.simplesource.saga.model.saga.SagaId;
import io.simplesource.saga.model.serdes.ActionSerdes;
import io.simplesource.saga.model.specs.ActionProcessorSpec;
import io.simplesource.saga.serialization.avro.AvroSerdes;
import io.simplesource.saga.shared.topics.TopicNamer;
import io.simplesource.saga.shared.topics.TopicTypes;
import io.simplesource.saga.shared.utils.StreamAppConfig;
import io.simplesource.saga.testutils.*;
import lombok.Value;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.streams.Topology;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

class SourcingStreamTests2 {

    private static String SCHEMA_URL = "http://localhost:8081/";
    private static String ACCOUNT_ID = "account id";

    @Value
    private static class AccountContext {
        final TestContext testContext;

        // serdes
        final ActionSerdes<SpecificRecord> actionSerdes = AvroSerdes.Specific.actionSerdes(SCHEMA_URL, true);
        final CommandSerdes<AccountId, AccountCommand> commandSerdes = AvroCommandSerdes.of(SCHEMA_URL, true);

        // publishers
        final RecordPublisher<SagaId, ActionRequest<SpecificRecord>> actionRequestPublisher;
        final RecordPublisher<AccountId, CommandResponse<AccountId>> commandResponsePublisher;
        final RecordPublisher<SagaId, ActionResponse> actionResponsePublisher;

        // verifiers
        final RecordVerifier<AccountId, CommandRequest<AccountId, AccountCommand>> commandRequestVerifier;
        final RecordVerifier<SagaId, ActionResponse> actionResponseVerifier;

        final Set<String> expectedTopics;

        AccountContext() {
            CommandSpec<SpecificRecord, AccountCommand, AccountId, AccountCommand> commandSpec = new CommandSpec<>(
                    Constants.ACCOUNT_ACTION_TYPE,
                    a -> Result.success((AccountCommand) a),
                    c -> c,
                    AccountCommand::getId,
                    c -> Sequence.position(c.getSequence()),
                    commandSerdes,
                    2000);

            StreamApp<ActionProcessorSpec<SpecificRecord>> streamApp = new StreamApp<>(ActionProcessorSpec.of(actionSerdes));

            String accountActionBaseName = "sourcing-" + Constants.ACCOUNT_AGGREGATE_NAME;

            streamApp.addTopologyStep(SourcingBuilder.sourcingSteps(
                    commandSpec,
                    TopicUtils.buildSteps(Constants.ACTION_TOPIC_PREFIX, accountActionBaseName),
                    TopicUtils.buildSteps(Constants.COMMAND_TOPIC_PREFIX, Constants.ACCOUNT_AGGREGATE_NAME)));

            Properties config = StreamAppConfig.getConfig(new StreamAppConfig("app-id", "http://localhost:9092"));

            StreamApp<?>.StreamBuild sb = streamApp.build(config);
            Topology topology = sb.topologySupplier.get();
            expectedTopics = sb.topicCreations.stream().map(x -> x.topicName).collect(Collectors.toSet());

            testContext = TestContextBuilder.of(topology).build();

            // get actionRequestPublisher
            actionRequestPublisher = testContext.publisher(
                    TopicNamer.forPrefix(Constants.ACTION_TOPIC_PREFIX, accountActionBaseName)
                            .apply(TopicTypes.ActionTopic.request),
                    actionSerdes.sagaId(),
                    actionSerdes.request());

            commandResponsePublisher = testContext.publisher(
                    TopicNamer.forPrefix(Constants.COMMAND_TOPIC_PREFIX, Constants.ACCOUNT_AGGREGATE_NAME)
                            .apply(TopicTypes.CommandTopic.response),
                    commandSerdes.aggregateKey(),
                    commandSerdes.commandResponse());

            actionResponsePublisher = testContext.publisher(
                    TopicNamer.forPrefix(Constants.ACTION_TOPIC_PREFIX, accountActionBaseName)
                            .apply(TopicTypes.ActionTopic.response),
                    actionSerdes.sagaId(),
                    actionSerdes.response());

            // get commandRequestVerifier
            commandRequestVerifier = testContext.verifier(
                    TopicNamer.forPrefix(Constants.COMMAND_TOPIC_PREFIX, Constants.ACCOUNT_AGGREGATE_NAME)
                            .apply(TopicTypes.CommandTopic.request),
                    commandSerdes.aggregateKey(),
                    commandSerdes.commandRequest());

            actionResponseVerifier = testContext.verifier(
                    TopicNamer.forPrefix(Constants.ACTION_TOPIC_PREFIX, accountActionBaseName)
                            .apply(TopicTypes.ActionTopic.response),
                    actionSerdes.sagaId(),
                    actionSerdes.response());

        }
    }

    private static ActionRequest<SpecificRecord> createRequest(SagaId sagaId, AccountCommand accountCommand, CommandId commandId) {
        ActionCommand<SpecificRecord> actionCommand = ActionCommand.of(commandId, accountCommand);
        return ActionRequest.<SpecificRecord>builder()
                .sagaId(sagaId)
                .actionId(ActionId.random())
                .actionCommand(actionCommand)
                .actionType(Constants.ACCOUNT_ACTION_TYPE)
                .build();
    }

    @Test
    void actionRequestGeneratesCommandRequest() {

        AccountContext acc = new AccountContext();

        assertThat(acc.expectedTopics).containsExactlyInAnyOrder(
                "saga_action_processor_sourcing-account-action_response",
                "saga_action_processor_sourcing-account-action_request",
                "saga_command_account-command_response",
                "saga_command_account-command_request");

        CreateAccount createAccount = new CreateAccount(ACCOUNT_ID, "user name");
        AccountCommand accountCommand = new AccountCommand(new AccountId(createAccount.getId()), 200L, createAccount);

        ActionRequest<SpecificRecord> actionRequest = createRequest(SagaId.random(), accountCommand, CommandId.random());

        acc.actionRequestPublisher.publish(actionRequest.sagaId, actionRequest);

        acc.commandRequestVerifier.verifySingle((accountId, commandRequest) -> {
            assertThat(commandRequest.readSequence().getSeq()).isEqualTo(200L);
            assertThat(accountId.getId()).isEqualTo(ACCOUNT_ID);
            assertThat(commandRequest.command()).isEqualToComparingFieldByField(accountCommand);
        });
        acc.commandRequestVerifier.verifyNoRecords();
    }

    @Test
    void commandResponseGeneratesActionResponse() {

        AccountContext acc = new AccountContext();

        CreateAccount createAccount = new CreateAccount(ACCOUNT_ID, "user name");
        AccountCommand accountCommand = new AccountCommand(new AccountId(createAccount.getId()), 200L, createAccount);

        CommandId commandId = CommandId.random();
        ActionRequest<SpecificRecord> actionRequest = createRequest(SagaId.random(), accountCommand, commandId);

        acc.actionRequestPublisher.publish(actionRequest.sagaId, actionRequest);
        acc.commandRequestVerifier.drainAll();

        CommandResponse<AccountId> commandResponse = new CommandResponse<>(commandId, accountCommand.getId(), Sequence.position(201L), Result.success(Sequence.position(202L)));
        acc.commandResponsePublisher.publish(new AccountId(createAccount.getId()), commandResponse);

        acc.actionResponseVerifier.verifySingle((sagaId, actionResponse) -> {
            assertThat(sagaId).isEqualTo(actionRequest.sagaId);
            assertThat(actionResponse.actionId).isEqualTo(actionRequest.actionId);
            assertThat(actionResponse.sagaId).isEqualTo(actionRequest.sagaId);
            assertThat(actionResponse.result.isSuccess()).isEqualTo(true);
        });
        acc.actionResponseVerifier.verifyNoRecords();
    }

    @Test
    void actionIndempotence() {

        AccountContext acc = new AccountContext();

        CreateAccount createAccount = new CreateAccount(ACCOUNT_ID, "user name");
        AccountCommand accountCommand = new AccountCommand(new AccountId(createAccount.getId()), 200L, createAccount);

        CommandId commandId = CommandId.random();
        ActionRequest<SpecificRecord> actionRequest = createRequest(SagaId.random(), accountCommand, commandId);

        acc.actionRequestPublisher.publish(actionRequest.sagaId, actionRequest);
        acc.commandRequestVerifier.drainAll();

        CommandResponse commandResponse = new CommandResponse<>(commandId, accountCommand.getId(), Sequence.position(201L), Result.success(Sequence.position(202L)));
        acc.commandResponsePublisher.publish(new AccountId(createAccount.getId()), commandResponse);

        acc.actionResponseVerifier.verifySingle((sagaId, actionResponse) -> {
        });
        acc.actionResponseVerifier.verifyNoRecords();

        acc.actionRequestPublisher.publish(actionRequest.sagaId, actionRequest);
        // does not generate a command request
        acc.commandRequestVerifier.verifyNoRecords();
        // regenerates an action response

        acc.actionResponseVerifier.verifySingle((sagaId, actionResponse) -> {
            assertThat(sagaId).isEqualTo(actionRequest.sagaId);
            assertThat(actionResponse.actionId).isEqualTo(actionRequest.actionId);
            assertThat(actionResponse.result.isSuccess()).isEqualTo(true);
        });
        acc.actionResponseVerifier.verifyNoRecords();
    }

    @Test
    void validSequenceIdsForAggregateSameSaga() {
        validateSequenceIdsForAggregate(true);
    }

    @Test
    void validSequenceIdsForAggregateDifferentSaga() {
        validateSequenceIdsForAggregate(false);
    }

    private void validateSequenceIdsForAggregate(boolean isSameSaga) {
        AccountContext acc = new AccountContext();

        CreateAccount createAccount = new CreateAccount(ACCOUNT_ID, "user name");
        AccountCommand createCommand = new AccountCommand(new AccountId(createAccount.getId()), 100L, createAccount);

        CommandId createCommandId = CommandId.random();
        SagaId sagaId = SagaId.random();
        ActionRequest<SpecificRecord> createActionRequest = createRequest(sagaId, createCommand, createCommandId);

        acc.actionRequestPublisher.publish(sagaId, createActionRequest);
        acc.commandRequestVerifier.drainAll();

        CommandResponse<AccountId> createCommandResponse = new CommandResponse<>(createCommandId, createCommand.getId(), Sequence.position(185L), Result.success(Sequence.position(186L)));
        acc.commandResponsePublisher.publish(new AccountId(createAccount.getId()), createCommandResponse);

        // let another saga try (or the same saga if isSameSaga = true)
        CommandId transferCommandId = CommandId.random();
        SagaId sagaId2 = isSameSaga ? sagaId : SagaId.random();
        AccountCommand transferCommand = new AccountCommand(createCommand.getId(), 186L, new TransferFunds(ACCOUNT_ID, "account id 2", 50.0));
        ActionRequest<SpecificRecord> transferRequest = createRequest(sagaId2, transferCommand, transferCommandId);
        acc.actionRequestPublisher.publish(sagaId2, transferRequest);

        acc.commandRequestVerifier.verifySingle((aId, cr) -> {
            assertThat(cr.readSequence().getSeq()).isEqualTo(186L); // get that from the previous response
        });

        acc.commandRequestVerifier.verifyNoRecords();
        CommandResponse<AccountId> transferCommandResponse = new CommandResponse<>(transferCommandId, transferCommand.getId(), Sequence.position(186L), Result.success(Sequence.position(187L)));
        acc.commandResponsePublisher.publish(new AccountId(createAccount.getId()), transferCommandResponse);

        // add another request, but don't know about the saga, so use the previous sequence number
        AccountCommand addCommand = new AccountCommand(createCommand.getId(), 0L, new AddFunds(ACCOUNT_ID, 100.0));
        ActionRequest<SpecificRecord> addRequest = createRequest(sagaId, addCommand, CommandId.random());
        acc.actionRequestPublisher.publish(sagaId, addRequest);

        acc.commandRequestVerifier.verifySingle((aId, cr) -> {
            assertThat(cr.readSequence().getSeq()).isEqualTo(isSameSaga ? 187L : 186L);
            assertThat(aId.getId()).isEqualTo(ACCOUNT_ID);
            assertThat(cr.command()).isEqualToComparingFieldByField(addCommand);
        });

        acc.commandRequestVerifier.verifyNoRecords();
    }
}
