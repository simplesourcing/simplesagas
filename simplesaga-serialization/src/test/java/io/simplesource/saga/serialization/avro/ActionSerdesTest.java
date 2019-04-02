package io.simplesource.saga.serialization.avro;

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.simplesource.api.CommandId;
import io.simplesource.data.Result;
import io.simplesource.kafka.internal.util.Tuple2;
import io.simplesource.saga.model.action.ActionCommand;
import io.simplesource.saga.model.action.ActionId;
import io.simplesource.saga.model.messages.ActionRequest;
import io.simplesource.saga.model.messages.ActionResponse;
import io.simplesource.saga.model.messages.UndoCommand;
import io.simplesource.saga.model.saga.SagaError;
import io.simplesource.saga.model.saga.SagaId;
import io.simplesource.saga.model.serdes.ActionSerdes;
import io.simplesource.saga.serialization.avro.generated.test.User;
import io.simplesource.saga.shared.serialization.TupleSerdes;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.serialization.Serde;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;


class ActionSerdesTest {
    private static String SCHEMA_URL = "http://localhost:8081/";
    private static String FAKE_TOPIC = "topic";

    @Test
    void actionIdTest() {
        ActionSerdes<?> serdes = AvroSerdes.Specific.actionSerdes(SCHEMA_URL, true);
        ActionId original = ActionId.random();
        byte[] serialized = serdes.actionId().serializer().serialize(FAKE_TOPIC, original);
        ActionId deserialized = serdes.actionId().deserializer().deserialize(FAKE_TOPIC, serialized);
        assertThat(deserialized).isEqualTo(original);
    }

    @Test
    void sagaIdTest() {
        ActionSerdes<?> serdes = AvroSerdes.Specific.actionSerdes(SCHEMA_URL, true);
        SagaId original = SagaId.random();
        byte[] serialized = serdes.sagaId().serializer().serialize(FAKE_TOPIC, original);
        SagaId deserialized = serdes.sagaId().deserializer().deserialize(FAKE_TOPIC, serialized);
        assertThat(deserialized).isEqualTo(original);
    }

    @Test
    void actionRequestSpecificTest() {
        Serde<User> payloadSerde = SpecificSerdeUtils.specificAvroSerde(SCHEMA_URL, false, new MockSchemaRegistryClient());
        ActionSerdes<User> serdes = AvroSerdes.actionSerdes(payloadSerde, SCHEMA_URL, true);
        User testUser = new User("Albus", "Dumbledore", 1732);

        ActionCommand<User> actionCommand = ActionCommand.of(CommandId.random(), testUser, "actionType");

        ActionRequest<User> original =
                ActionRequest.of(SagaId.random(), ActionId.random(), actionCommand, false);

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

        ActionCommand<User> actionCommand = ActionCommand.of(CommandId.random(), testUser, "actionType");

        ActionRequest<User> original =
                ActionRequest.of(SagaId.random(), ActionId.random(), actionCommand, false);

        byte[] serialized = serdes.request().serializer().serialize(FAKE_TOPIC, original);
        ActionRequest<User> deserialized = serdes.request().deserializer().deserialize(FAKE_TOPIC, serialized);
        assertThat(deserialized).isEqualToIgnoringGivenFields(original, "actionCommand");

        String originalStr = original.toString();
        assertThat(deserialized.toString()).isEqualTo(originalStr);
    }

    @Test
    void responseTestSuccessEmpty() {
        ActionSerdes<SpecificRecord> serdes = AvroSerdes.Specific.actionSerdes(SCHEMA_URL, true);
        ActionResponse<SpecificRecord> original = ActionResponse.of(SagaId.random(), ActionId.random(), CommandId.random(), Result.success(Optional.empty()));
        byte[] serialized = serdes.response().serializer().serialize(FAKE_TOPIC, original);
        ActionResponse deserialized = serdes.response().deserializer().deserialize(FAKE_TOPIC, serialized);
        assertThat(deserialized).isEqualToIgnoringGivenFields(original, "result");
        assertThat(deserialized.result.isSuccess()).isTrue();
    }

    @Test
    void responseTestSuccessWithUndo() {
        UndoCommand undoCommand = UndoCommand.of(new User("Albus", "Dumbledore", 1732), "action_type");
        ActionSerdes<SpecificRecord> serdes = AvroSerdes.Specific.actionSerdes(SCHEMA_URL, true);
        ActionResponse<SpecificRecord> original = ActionResponse.of(SagaId.random(), ActionId.random(), CommandId.random(), Result.success(Optional.of(undoCommand)));
        byte[] serialized = serdes.response().serializer().serialize(FAKE_TOPIC, original);
        ActionResponse deserialized = serdes.response().deserializer().deserialize(FAKE_TOPIC, serialized);
        assertThat(deserialized).isEqualToIgnoringGivenFields(original, "result");
        assertThat(deserialized.result.isSuccess()).isTrue();
        assertThat(deserialized.result.getOrElse(null)).isEqualTo(Optional.of(undoCommand));
    }

    @Test
    void responseTestFailure() {
        ActionSerdes<SpecificRecord> serdes = AvroSerdes.Specific.actionSerdes(SCHEMA_URL, true);
        SagaError sagaError1 = SagaError.of(SagaError.Reason.InternalError, "There was an error");
        SagaError sagaError2 = SagaError.of(SagaError.Reason.CommandError, "Invalid command");
        ActionResponse<SpecificRecord> original = ActionResponse.of(SagaId.random(), ActionId.random(), CommandId.random(),
                Result.failure(sagaError1, sagaError2));
        byte[] serialized = serdes.response().serializer().serialize(FAKE_TOPIC, original);
        ActionResponse<SpecificRecord> deserialized = serdes.response().deserializer().deserialize(FAKE_TOPIC, serialized);
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
        SagaId sagaId = SagaId.random();
        Serde<User> payloadSerde = SpecificSerdeUtils.specificAvroSerde(SCHEMA_URL, false, new MockSchemaRegistryClient());
        ActionSerdes<User> serdes = AvroSerdes.actionSerdes(payloadSerde, SCHEMA_URL, true);
        User testUser = new User("Albus", "Dumbledore", 1732);

        ActionCommand<User> actionCommand = ActionCommand.of(CommandId.random(), testUser, "actionType");

        ActionRequest<User> request =
                ActionRequest.of(SagaId.random(), ActionId.random(), actionCommand, false);

        Tuple2<SagaId, ActionRequest<User>> tuple2 = Tuple2.of(sagaId, request);

        Serde<Tuple2<SagaId, ActionRequest<User>>> tupleSerde = TupleSerdes.tuple2(serdes.sagaId(), serdes.request());

        byte[] serialized = tupleSerde.serializer().serialize(FAKE_TOPIC, tuple2);
        Tuple2<SagaId, ActionRequest<User>> deserialized = tupleSerde.deserializer().deserialize(FAKE_TOPIC, serialized);
        assertThat(deserialized.v2()).isEqualToIgnoringGivenFields(request, "actionCommand");

        User uo = request.actionCommand.command;
        User ud = deserialized.v2().actionCommand.command;

        assertThat(deserialized.v1()).isEqualTo(sagaId);
        assertThat(deserialized.v2().actionCommand).isEqualToIgnoringGivenFields(request.actionCommand, "command");
        assertThat(ud.getFirstName()).isEqualTo(uo.getFirstName());
        assertThat(ud.getLastName()).isEqualTo(uo.getLastName());
        assertThat(ud.getYearOfBirth()).isEqualTo(uo.getYearOfBirth());
    }

}
