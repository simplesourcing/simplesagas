package io.simplesource.saga.serialization.avro;

import io.simplesource.api.CommandId;
import io.simplesource.saga.model.action.ActionCommand;
import io.simplesource.saga.model.action.ActionId;
import io.simplesource.saga.model.messages.ActionRequest;
import io.simplesource.saga.model.saga.SagaId;
import io.simplesource.saga.model.serdes.ActionSerdes;
import io.simplesource.saga.serialization.avro.generated.test.User;
import net.jqwik.api.*;
import net.jqwik.api.arbitraries.StringArbitrary;

import static org.assertj.core.api.Assertions.assertThat;

// TODO: replace all Serdes tests with property based tests as below
// TODO: create a custom Arbitrary for generating any generic record (with a generated schema)
class ActionSerdesPropsTest {
    private static String SCHEMA_URL = "http://localhost:8081/";
    private static String FAKE_TOPIC = "topic";
    ActionSerdes<User> serdes = AvroSerdes.Specific.actionSerdes(SCHEMA_URL, true);

    @Property
    boolean ActionRequest_serializes_and_deserializes(@ForAll("userActionRequest") ActionRequest<User> original) {

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

        Arbitrary<SagaId> sagaId = Arbitraries.randomValue(random -> SagaId.random());
        Arbitrary<ActionId> actionId = Arbitraries.randomValue(random -> ActionId.random());
        Arbitrary<CommandId> commandId = Arbitraries.randomValue(random -> CommandId.random());

        Arbitrary<ActionCommand<User>> actionCommand = Combinators.combine(
            commandId,
            user,
            strings
        ).as(ActionCommand::of);
        return Combinators.combine(sagaId, actionId, actionCommand, strings).as(
                (sId,aId, command, actionType) ->
                        ActionRequest.of(sId, aId, command, false));
    }
}
