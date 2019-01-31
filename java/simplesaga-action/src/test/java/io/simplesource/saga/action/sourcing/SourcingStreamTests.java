package io.simplesource.saga.action.sourcing;

import io.simplesource.data.Result;
import io.simplesource.kafka.api.CommandSerdes;
import io.simplesource.kafka.model.CommandRequest;
import io.simplesource.kafka.serialization.avro.AvroCommandSerdes;
import io.simplesource.saga.avro.avro.generated.test.AccountCommand;
import io.simplesource.saga.avro.avro.generated.test.AccountId;
import io.simplesource.saga.avro.avro.generated.test.CreateAccount;
import io.simplesource.saga.model.action.ActionCommand;
import io.simplesource.saga.model.messages.ActionRequest;
import io.simplesource.saga.model.serdes.ActionSerdes;
import io.simplesource.saga.serialization.avro.AvroSerdes;
import io.simplesource.saga.shared.topics.TopicNamer;
import io.simplesource.saga.shared.topics.TopicTypes;
import io.simplesource.saga.testutils.*;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.streams.Topology;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

public class SourcingStreamTests {
    private static Logger logger = LoggerFactory.getLogger(SourcingStreamTests.class);

    private static String SCHEMA_URL = "http://localhost:8081/";

    @Test
    void processActionRequest() {

        ActionSerdes<SpecificRecord> actionSerdes = AvroSerdes.actionSerdes(SCHEMA_URL, true);
        CommandSerdes<AccountId, AccountCommand> commandSerdes = AvroCommandSerdes.of(SCHEMA_URL, true);

        CommandSpec<SpecificRecord, AccountCommand, AccountId, AccountCommand> commandSpec = new CommandSpec<>(
                Constants.accountActionType,
                a -> Result.success((AccountCommand) a),
                c -> c,
                AccountCommand::getId,
                commandSerdes,
                Constants.accountAggregateName,
                2000);

        SourcingApp<SpecificRecord> sourcingApp = new SourcingApp<>(actionSerdes,
                TopicUtils.buildSteps("", Constants.actionTopicPrefix));
        sourcingApp.addCommand(commandSpec, TopicUtils.buildSteps("", Constants.commandTopicPrefix));

        assertThat(sourcingApp).isEqualTo(sourcingApp);
        Topology topology = sourcingApp.buildTopology();

        assertThat(topology).isEqualTo(topology);

        TestContext testContext = TestContextBuilder.of(topology).build();

        // get publisher
        String actionRequestTopic = TopicNamer.forPrefix("", Constants.actionTopicPrefix).apply(TopicTypes.ActionTopic.request);
        RecordPublisher<UUID, ActionRequest<SpecificRecord>> publisher = testContext.topicPublisher(actionRequestTopic, actionSerdes.uuid(), actionSerdes.request());

        // get verifier
        String commandRequestTopic = TopicNamer.forPrefix(Constants.accountAggregateName, Constants.commandTopicPrefix).apply(TopicTypes.CommandTopic.request);
        RecordVerifier<AccountId, CommandRequest<AccountId, AccountCommand>> verifier = testContext.verifier(commandRequestTopic, commandSerdes.aggregateKey(), commandSerdes.commandRequest());

        CreateAccount createAccount = new CreateAccount("account id", "user name");
        AccountCommand accountCommand = new AccountCommand(new AccountId(createAccount.getId()), createAccount);

        ActionCommand<SpecificRecord> actionCommand = new ActionCommand<>(UUID.randomUUID(), accountCommand);
        ActionRequest<SpecificRecord> actionRequest = ActionRequest.<SpecificRecord>builder()
                .sagaId(UUID.randomUUID())
                .actionId(UUID.randomUUID())
                .actionCommand(actionCommand)
                .actionType(Constants.accountActionType)
                .build();

        publisher.publish(actionRequest.sagaId, actionRequest);

        verifier.drainAll();
    }
}
