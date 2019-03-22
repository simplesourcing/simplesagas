package io.simplesource.saga.serialization.avro;

import io.simplesource.api.CommandId;
import io.simplesource.saga.model.action.ActionCommand;
import io.simplesource.saga.model.messages.ActionRequest;
import io.simplesource.saga.model.serdes.ActionSerdes;
import io.simplesource.saga.serialization.avro.generated.test.User;
import net.jqwik.api.*;
import net.jqwik.api.arbitraries.StringArbitrary;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

// TODO: replace all Serdes tests with property based tests as below
// TODO: create a custom Arbitrary for generating any generic record (with a generated schema)
class ActionSerdesPropsTest {
    private static String SCHEMA_URL = "http://localhost:8081/";
    private static String FAKE_TOPIC = "topic";
    ActionSerdes<User> serdes = AvroSerdes.Specific.actionSerdes(SCHEMA_URL, true);

    @Property
    boolean ActionRequest_serialises_and_deserialises(@ForAll("userActionRequest") ActionRequest<User> original) {

        byte[] serialized = serdes.request().serializer().serialize(FAKE_TOPIC, original);
        ActionRequest<User> deserialized = serdes.request().deserializer().deserialize(FAKE_TOPIC, serialized);
        assertThat(deserialized).isEqualToIgnoringGivenFields(original, "actionCommand");

        String originalStr = original.toString();
        assertThat(deserialized.toString()).isEqualTo(originalStr);
        return true;
    }

    @Provide
    Arbitrary<ActionRequest<User>> userActionRequest() {
        StringArbitrary strings = Arbitraries.strings().withCharRange('0', 'z');

        Arbitrary<User> user = Combinators.combine(
                strings,
                strings,
                Arbitraries.integers()
        ).as(User::new);

        Arbitrary<UUID> uuid = Arbitraries.randomValue(random -> UUID.randomUUID());
        Arbitrary<CommandId> commandId = Arbitraries.randomValue(random -> CommandId.random());

        Arbitrary<ActionCommand<User>> actionCommand = Combinators.combine(
            commandId,
            user
        ).as(ActionCommand::new);
        return Combinators.combine(uuid, uuid, actionCommand, strings).as(
                (sagaId,actionId, command, actionType) -> ActionRequest.<User>builder()
            .sagaId(sagaId)
            .actionId(actionId)
            .actionCommand(command)
            .actionType(actionType)
            .build()
        );
    }
}
