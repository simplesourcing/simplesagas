package io.simplesource.saga.serialization.avro;

import io.simplesource.saga.model.action.ActionStatus;
import io.simplesource.saga.model.messages.SagaStateTransition;
import io.simplesource.saga.model.saga.Saga;
import io.simplesource.saga.model.saga.SagaError;
import io.simplesource.saga.model.saga.SagaStatus;
import io.simplesource.saga.model.serdes.SagaSerdes;
import io.simplesource.saga.shared.utils.Lists;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class SagaInternalSerdesTest {

    private static String SCHEMA_URL = "http://localhost:8081/";
    private static String FAKE_TOPIC = "topic";

    @Test
    void sagaStateTest() {
        SagaSerdes<GenericRecord> serdes = AvroSerdes.sagaSerdes(SCHEMA_URL, true);

        Saga<GenericRecord> original = SagaTestUtils.getTestSaga();

        byte[] serialized = serdes.state().serializer().serialize(FAKE_TOPIC, original);
        Saga<GenericRecord> deserialized = serdes.state().deserializer().deserialize(FAKE_TOPIC, serialized);

        String originalAsString = original.toString();
        assertThat(deserialized.toString()).hasSameSizeAs(originalAsString);
        SagaTestUtils.validataSaga(deserialized, original);
    }

    <A extends SagaStateTransition> A testTransition(SagaStateTransition transition) {
        SagaSerdes<GenericRecord> serdes = AvroSerdes.sagaSerdes(SCHEMA_URL, true);

        byte[] serialized = serdes.transition().serializer().serialize(FAKE_TOPIC, transition);
        SagaStateTransition deserialized = serdes.transition().deserializer().deserialize(FAKE_TOPIC, serialized);

        String originalAsString = transition.toString();
        assertThat(deserialized.toString()).hasSameSizeAs(originalAsString);

        return (A)deserialized;
    }

    @Test
    void sagaTransitionInitialTest() {
        Saga<GenericRecord> testSaga = SagaTestUtils.getTestSaga();
        SagaStateTransition.SetInitialState<GenericRecord> original = new SagaStateTransition.SetInitialState<>(testSaga);

        SagaStateTransition.SetInitialState deserialized = testTransition(original);
        SagaTestUtils.validataSaga(deserialized.sagaState, original.sagaState);

    }

    @Test
    void sagaTransitionActionStatusSuccessTest() {
        SagaStateTransition.SagaActionStatusChanged original = new SagaStateTransition.SagaActionStatusChanged(
                UUID.randomUUID(),
                UUID.randomUUID(),
                ActionStatus.Completed,
                Collections.emptyList());

        testTransition(original);
    }

    @Test
    void sagaTransitionActionStatusFailureTest() {
        SagaStateTransition.SagaActionStatusChanged original = new SagaStateTransition.SagaActionStatusChanged(
                UUID.randomUUID(),
                UUID.randomUUID(),
                ActionStatus.Failed,
                Lists.of(
                        SagaError.of(SagaError.Reason.InternalError, "internal error"),
                        SagaError.of(SagaError.Reason.Timeout, "timeout")));

        testTransition(original);
    }

    @Test
    void sagaTransitionSagaStatusSuccessTest() {
        SagaStateTransition.SagaStatusChanged original = new SagaStateTransition.SagaStatusChanged(
                UUID.randomUUID(),
                SagaStatus.Completed,
                Collections.emptyList());

        testTransition(original);
    }

    @Test
    void sagaTransitionSagaStatusFailureTest() {
        SagaStateTransition.SagaStatusChanged original = new SagaStateTransition.SagaStatusChanged(
                UUID.randomUUID(),
                SagaStatus.Completed,
                Lists.of(
                        SagaError.of(SagaError.Reason.InternalError, "internal error"),
                        SagaError.of(SagaError.Reason.Timeout, "timeout")));

        testTransition(original);
    }

    @Test
    void sagaTransitionTransitionListTest() {
        SagaStateTransition.TransitionList original = new SagaStateTransition.TransitionList(Lists.of(
                new SagaStateTransition.SagaActionStatusChanged(
                        UUID.randomUUID(),
                        UUID.randomUUID(),
                        ActionStatus.Failed,
                        Lists.of(
                                SagaError.of(SagaError.Reason.InternalError, "internal error"),
                                SagaError.of(SagaError.Reason.Timeout, "timeout"))),
                new SagaStateTransition.SagaActionStatusChanged(
                        UUID.randomUUID(),
                        UUID.randomUUID(),
                        ActionStatus.Completed,
                        Collections.emptyList())));

        testTransition(original);
    }
}
