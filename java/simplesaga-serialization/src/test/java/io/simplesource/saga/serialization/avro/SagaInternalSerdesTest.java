package io.simplesource.saga.serialization.avro;

import io.simplesource.saga.model.action.ActionStatus;
import io.simplesource.saga.model.messages.SagaStateTransition;
import io.simplesource.saga.model.saga.Saga;
import io.simplesource.saga.model.saga.SagaError;
import io.simplesource.saga.model.saga.SagaStatus;
import io.simplesource.saga.model.serdes.SagaSerdes;
import io.simplesource.saga.shared.utils.Lists;
import org.apache.avro.specific.SpecificRecord;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class SagaInternalSerdesTest {

    private static String SCHEMA_URL = "http://localhost:8081/";
    private static String FAKE_TOPIC = "topic";

    @Test
    void sagaStateTest() {
        SagaSerdes<SpecificRecord> serdes = AvroSerdes.Specific.sagaSerdes(SCHEMA_URL, true);

        Saga<SpecificRecord> original = SagaTestUtils.getTestSaga();

        byte[] serialized = serdes.state().serializer().serialize(FAKE_TOPIC, original);
        Saga<SpecificRecord> deserialized = serdes.state().deserializer().deserialize(FAKE_TOPIC, serialized);

        String originalAsString = original.toString();
        assertThat(deserialized.toString()).hasSameSizeAs(originalAsString);
        SagaTestUtils.validataSaga(deserialized, original);
    }

    <A extends SagaStateTransition> A testTransition(SagaStateTransition transition) {
        SagaSerdes<SpecificRecord> serdes = AvroSerdes.Specific.sagaSerdes(SCHEMA_URL, true);

        byte[] serialized = serdes.transition().serializer().serialize(FAKE_TOPIC, transition);
        SagaStateTransition deserialized = serdes.transition().deserializer().deserialize(FAKE_TOPIC, serialized);

        String originalAsString = transition.toString();
        assertThat(deserialized.toString()).hasSameSizeAs(originalAsString);

        A aDes = (A)deserialized;
        assertThat(aDes).isNotNull();
        return aDes;
    }

    @Test
    void sagaTransitionInitialTest() {
        Saga<SpecificRecord> testSaga = SagaTestUtils.getTestSaga();
        SagaStateTransition.SetInitialState<SpecificRecord> original = new SagaStateTransition.SetInitialState<>(testSaga);

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

        SagaStateTransition.SagaActionStatusChanged deserialized = testTransition(original);
        assertThat(deserialized).isEqualToComparingFieldByField(original);
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

        SagaStateTransition.SagaActionStatusChanged deserialized = testTransition(original);
        assertThat(deserialized).isEqualToComparingFieldByField(original);
    }

    @Test
    void sagaTransitionSagaStatusSuccessTest() {
        SagaStateTransition.SagaStatusChanged original = new SagaStateTransition.SagaStatusChanged(
                UUID.randomUUID(),
                SagaStatus.Completed,
                Collections.emptyList());

        testTransition(original);

        SagaStateTransition.SagaStatusChanged deserialized = testTransition(original);
        assertThat(deserialized).isEqualToComparingFieldByField(original);
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

        SagaStateTransition.SagaStatusChanged deserialized = testTransition(original);
        assertThat(deserialized).isEqualToComparingFieldByField(original);
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
