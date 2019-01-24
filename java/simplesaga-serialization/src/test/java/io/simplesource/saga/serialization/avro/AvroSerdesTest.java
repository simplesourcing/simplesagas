package io.simplesource.saga.serialization.avro;

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.simplesource.saga.model.action.ActionCommand;
import io.simplesource.saga.model.messages.ActionRequest;
import io.simplesource.saga.model.serdes.ActionSerdes;
import io.simplesource.saga.serialization.avro.generated.test.User;
import org.apache.kafka.common.serialization.Serde;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class AvroSerdesTest {

    static String SCHEMA_URL = "http://localhost:8081/";
    static String FAKE_TOPIC = "topic";


    @Test
    void uuidTest() {

        ActionSerdes<?> serdes = AvroSerdes.actionSerdes(SCHEMA_URL, true);
        UUID original = UUID.randomUUID();
        byte[] serialized = serdes.uuid().serializer().serialize(FAKE_TOPIC, original);
        UUID deserialized = serdes.uuid().deserializer().deserialize(FAKE_TOPIC, serialized);
        assertThat(deserialized).isEqualTo(original);
    }

    @Test
    void actionRequestTestSpecific() {
        Serde<User> payloadSerde = SpecificSerdeUtils.specificAvroSerde(SCHEMA_URL, false, new MockSchemaRegistryClient());
        ActionSerdes<User> serdes = AvroSerdes.actionSerdes(payloadSerde, SCHEMA_URL, true);
        User testUser = new User("Albus", "Dumbledore", 1732);

        ActionCommand<User> actionCommand = new ActionCommand<>(UUID.randomUUID(), testUser);

        ActionRequest<User> original = ActionRequest.<User>builder()
                .sagaId(UUID.randomUUID())
                .actionId(UUID.randomUUID())
                .actionCommand(actionCommand)
                .actionType("actionType")
                .build();

        byte[] serialized = serdes.request().serializer().serialize(FAKE_TOPIC, original);
        ActionRequest<User> deserialized = serdes.request().deserializer().deserialize(FAKE_TOPIC, serialized);
        assertThat(deserialized).isEqualToIgnoringGivenFields(original, "actionCommand");

        User uo = original.actionCommand.command;
        User ud = deserialized.actionCommand.command;

        assertThat(deserialized.actionCommand).isEqualToIgnoringGivenFields(original.actionCommand, "command");
        assertThat(ud.getFirstName()).isEqualTo(uo.getFirstName());
        assertThat(ud.getLastName()).isEqualTo(uo.getLastName());
        assertThat(ud.getYearOfBirth()).isEqualTo(uo.getYearOfBirth());
    }
}
