package io.simplesource.saga.serialization.avro;

import io.simplesource.saga.model.action.ActionId;
import io.simplesource.saga.model.action.ActionStatus;
import io.simplesource.saga.model.messages.SagaStateTransition;
import io.simplesource.saga.model.messages.UndoCommand;
import io.simplesource.saga.model.saga.Saga;
import io.simplesource.saga.model.saga.SagaError;
import io.simplesource.saga.model.saga.SagaId;
import io.simplesource.saga.model.saga.SagaStatus;
import io.simplesource.saga.model.serdes.SagaSerdes;
import io.simplesource.saga.serialization.avro.generated.test.TransferFunds;
import io.simplesource.saga.shared.utils.Lists;
import org.apache.avro.specific.SpecificRecord;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Optional;

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

    <B extends SagaStateTransition<SpecificRecord>> B testTransition(SagaStateTransition<SpecificRecord> transition) {
        SagaSerdes<SpecificRecord> serdes = AvroSerdes.Specific.sagaSerdes(SCHEMA_URL, true);

        byte[] serialized = serdes.transition().serializer().serialize(FAKE_TOPIC, transition);
        SagaStateTransition deserialized = serdes.transition().deserializer().deserialize(FAKE_TOPIC, serialized);

        String originalAsString = transition.toString();
        assertThat(deserialized.toString()).hasSameSizeAs(originalAsString);

        B aDes = (B)deserialized;
        assertThat(aDes).isNotNull();
        return aDes;
    }

    @Test
    void sagaTransitionInitialTest() {
        Saga<SpecificRecord> testSaga = SagaTestUtils.getTestSaga();
        SagaStateTransition.SetInitialState<SpecificRecord> original = SagaStateTransition.SetInitialState.of(testSaga);

        SagaStateTransition.SetInitialState deserialized = testTransition(original);
        SagaTestUtils.validataSaga(deserialized.sagaState, original.sagaState);

    }

    @Test
    void sagaTransitionActionStatusSuccessTest() {
        SagaStateTransition.SagaActionStateChanged<SpecificRecord> original = SagaStateTransition.SagaActionStateChanged.of(
                SagaId.random(),
                ActionId.random(),
                ActionStatus.Completed,
                Collections.emptyList(),
                Optional.empty());

        SagaStateTransition.SagaActionStateChanged<SpecificRecord> deserialized = testTransition(original);
        assertThat(deserialized).isEqualToComparingFieldByField(original);
    }

    @Test
    void sagaTransitionActionStatusFailureTest() {
        SagaStateTransition.SagaActionStateChanged<SpecificRecord> original = SagaStateTransition.SagaActionStateChanged.of(
                SagaId.random(),
                ActionId.random(),
                ActionStatus.Failed,
                Lists.of(
                        SagaError.of(SagaError.Reason.InternalError, "internal error"),
                        SagaError.of(SagaError.Reason.Timeout, "timeout")),
                Optional.empty());

        testTransition(original);

        SagaStateTransition.SagaActionStateChanged<SpecificRecord> deserialized = testTransition(original);
        assertThat(deserialized).isEqualToComparingFieldByField(original);
    }

    @Test
    void sagaTransitionSagaStatusSuccessTest() {
        SagaStateTransition.SagaStatusChanged<SpecificRecord> original = SagaStateTransition.SagaStatusChanged.of(
                SagaId.random(),
                SagaStatus.Completed,
                Collections.emptyList());

        testTransition(original);

        SagaStateTransition.SagaStatusChanged<SpecificRecord> deserialized = testTransition(original);
        assertThat(deserialized).isEqualToComparingFieldByField(original);
    }

    @Test
    void sagaTransitionSagaStatusFailureTest() {
        SagaStateTransition.SagaStatusChanged<SpecificRecord> original = SagaStateTransition.SagaStatusChanged.of(
                SagaId.random(),
                SagaStatus.Completed,
                Lists.of(
                        SagaError.of(SagaError.Reason.InternalError, "internal error"),
                        SagaError.of(SagaError.Reason.Timeout, "timeout")));

        testTransition(original);

        SagaStateTransition.SagaStatusChanged<SpecificRecord> deserialized = testTransition(original);
        assertThat(deserialized).isEqualToComparingFieldByField(original);
    }

    @Test
    void sagaTransitionTransitionListTest() {
        SagaStateTransition.TransitionList<SpecificRecord> original = SagaStateTransition.TransitionList.of(Lists.of(
                SagaStateTransition.SagaActionStateChanged.of(
                        SagaId.random(),
                        ActionId.random(),
                        ActionStatus.Failed,
                        Lists.of(
                                SagaError.of(SagaError.Reason.InternalError, "internal error"),
                                SagaError.of(SagaError.Reason.Timeout, "timeout")),
                        Optional.empty()),
                SagaStateTransition.SagaActionStateChanged.of(
                        SagaId.random(),
                        ActionId.random(),
                        ActionStatus.Completed,
                        Collections.emptyList(),
                        Optional.empty())));

        testTransition(original);
    }

    @Test
    void sagaTransitionTransitionUndoActionTest() {
        SagaStateTransition.TransitionList<SpecificRecord> original = SagaStateTransition.TransitionList.of(Lists.of(
                SagaStateTransition.SagaActionStateChanged.of(
                        SagaId.random(),
                        ActionId.random(),
                        ActionStatus.Failed,
                        Lists.of(
                                SagaError.of(SagaError.Reason.InternalError, "internal error"),
                                SagaError.of(SagaError.Reason.Timeout, "timeout")),
                        Optional.of(UndoCommand.of(new TransferFunds("id1", "id2", 50.0), "action_undo_type"))),
                SagaStateTransition.SagaActionStateChanged.of(
                        SagaId.random(),
                        ActionId.random(),
                        ActionStatus.Completed,
                        Collections.emptyList(),
                        Optional.of(UndoCommand.of(new TransferFunds("id1", "id2", 50.0), "action_undo_type")))));

        testTransition(original);
    }
}
