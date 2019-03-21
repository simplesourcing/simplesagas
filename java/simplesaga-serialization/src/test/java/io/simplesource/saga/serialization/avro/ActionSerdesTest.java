package io.simplesource.saga.serialization.avro;

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.simplesource.data.Result;
import io.simplesource.kafka.internal.util.Tuple2;
import io.simplesource.saga.model.action.ActionCommand;
import io.simplesource.saga.model.messages.ActionRequest;
import io.simplesource.saga.model.messages.ActionResponse;
import io.simplesource.saga.model.saga.SagaError;
import io.simplesource.saga.model.serdes.ActionSerdes;
import io.simplesource.saga.serialization.avro.generated.test.User;
import io.simplesource.saga.shared.serialization.TupleSerdes;
import org.apache.kafka.common.serialization.Serde;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;


class ActionSerdesTest {
    private static Logger logger = LoggerFactory.getLogger(ActionSerdesTest.class);

    private static String SCHEMA_URL = "http://localhost:8081/";
    private static String FAKE_TOPIC = "topic";

    @Test
    void uuidTest() {
        ActionSerdes<?> serdes = AvroSerdes.Specific.actionSerdes(SCHEMA_URL, true);
        UUID original = UUID.randomUUID();
        byte[] serialized = serdes.uuid().serializer().serialize(FAKE_TOPIC, original);
        UUID deserialized = serdes.uuid().deserializer().deserialize(FAKE_TOPIC, serialized);
        assertThat(deserialized).isEqualTo(original);
    }

    @Test
    void actionRequestSpecificTest() {
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

    @Test
    void actionRequestGenericTest() {
        ActionSerdes<User> serdes = AvroSerdes.Specific.actionSerdes(SCHEMA_URL, true);
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

        String originalStr = original.toString();
        assertThat(deserialized.toString()).isEqualTo(originalStr);
    }

    @Test
    void responseTestSuccess() {
        ActionSerdes<?> serdes = AvroSerdes.Specific.actionSerdes(SCHEMA_URL, true);
        ActionResponse original = new ActionResponse(UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID(), Result.success(true));
        byte[] serialized = serdes.response().serializer().serialize(FAKE_TOPIC, original);
        ActionResponse deserialized = serdes.response().deserializer().deserialize(FAKE_TOPIC, serialized);
        assertThat(deserialized).isEqualToIgnoringGivenFields(original, "result");
        assertThat(deserialized.result.isSuccess()).isTrue();
    }

    @Test
    void responseTestFailure() {
        ActionSerdes<?> serdes = AvroSerdes.Specific.actionSerdes(SCHEMA_URL, true);
        SagaError sagaError1 = SagaError.of(SagaError.Reason.InternalError, "There was an error");
        SagaError sagaError2 = SagaError.of(SagaError.Reason.CommandError, "Invalid command");
        ActionResponse original = new ActionResponse(UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID(),
                Result.failure(sagaError1, sagaError2));
        byte[] serialized = serdes.response().serializer().serialize(FAKE_TOPIC, original);
        ActionResponse deserialized = serdes.response().deserializer().deserialize(FAKE_TOPIC, serialized);
        assertThat(deserialized).isEqualToIgnoringGivenFields(original, "result");
        assertThat(deserialized.result.isFailure()).isTrue();
        deserialized.result.failureReasons().ifPresent(nel -> {
            List<SagaError> el = nel.toList();
            assertThat(el).hasSize(2);
            assertThat(el.get(0)).isEqualToComparingFieldByField(sagaError1);
            assertThat(el.get(1)).isEqualToComparingFieldByField(sagaError2);
        });
    }

    @Test
    void tuple2Test() {
        UUID uuid = UUID.randomUUID();
        Serde<User> payloadSerde = SpecificSerdeUtils.specificAvroSerde(SCHEMA_URL, false, new MockSchemaRegistryClient());
        ActionSerdes<User> serdes = AvroSerdes.actionSerdes(payloadSerde, SCHEMA_URL, true);
        User testUser = new User("Albus", "Dumbledore", 1732);

        ActionCommand<User> actionCommand = new ActionCommand<>(UUID.randomUUID(), testUser);

        ActionRequest<User> request = ActionRequest.<User>builder()
                .sagaId(UUID.randomUUID())
                .actionId(UUID.randomUUID())
                .actionCommand(actionCommand)
                .actionType("actionType")
                .build();

        Tuple2<UUID, ActionRequest<User>> tuple2 = Tuple2.of(uuid, request);

        Serde<Tuple2<UUID, ActionRequest<User>>> tupleSerde = TupleSerdes.tuple2(serdes.uuid(), serdes.request());

        byte[] serialized =tupleSerde.serializer().serialize(FAKE_TOPIC, tuple2);
        Tuple2<UUID, ActionRequest<User>> deserialized = tupleSerde.deserializer().deserialize(FAKE_TOPIC, serialized);
        assertThat(deserialized.v2()).isEqualToIgnoringGivenFields(request, "actionCommand");

        User uo = request.actionCommand.command;
        User ud = deserialized.v2().actionCommand.command;

        assertThat(deserialized.v1()).isEqualTo(uuid);
        assertThat(deserialized.v2().actionCommand).isEqualToIgnoringGivenFields(request.actionCommand, "command");
        assertThat(ud.getFirstName()).isEqualTo(uo.getFirstName());
        assertThat(ud.getLastName()).isEqualTo(uo.getLastName());
        assertThat(ud.getYearOfBirth()).isEqualTo(uo.getYearOfBirth());
    }

}
