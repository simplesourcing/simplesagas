package io.simplesource.saga.serialization.avro;

import io.simplesource.api.CommandId;
import io.simplesource.data.Result;
import io.simplesource.saga.model.action.ActionCommand;
import io.simplesource.saga.model.action.ActionId;
import io.simplesource.saga.model.action.SagaAction;
import io.simplesource.saga.model.saga.Saga;
import io.simplesource.saga.model.saga.SagaError;
import io.simplesource.saga.client.dsl.SagaDSL;
import io.simplesource.saga.serialization.avro.generated.test.AddFunds;
import io.simplesource.saga.serialization.avro.generated.test.CreateAccount;
import io.simplesource.saga.serialization.avro.generated.test.TransferFunds;
import org.apache.avro.specific.SpecificRecord;

import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;

import static io.simplesource.saga.client.dsl.SagaDSL.inParallel;
import static org.assertj.core.api.Assertions.assertThat;

public class SagaTestUtils {

    static Saga<SpecificRecord> getTestSaga() {
        SagaDSL.SagaBuilder<SpecificRecord> builder = SagaDSL.createBuilder();

        Function<SpecificRecord, SagaDSL.SubSaga<SpecificRecord>> addAction = command ->
                builder.addAction("actionType", command);

        BiFunction<SpecificRecord, SpecificRecord, SagaDSL.SubSaga<SpecificRecord>> addActionWithUndo = (command, undo) ->
                builder.addAction(
                        ActionId.random(),
                        "actionType",
                        command,
                        undo);

        SagaDSL.SubSaga<SpecificRecord> create1 = addAction.apply(new CreateAccount("id1", "User 1"));
        SagaDSL.SubSaga<SpecificRecord> create2 = addAction.apply(new CreateAccount("id2", "User 2"));

        SagaDSL.SubSaga<SpecificRecord> add = addActionWithUndo.apply(
                new AddFunds("id1", 1000.0),
                new AddFunds("id1", -1000.0));
        SagaDSL.SubSaga<SpecificRecord> transfer = addActionWithUndo.apply(
                new TransferFunds("id1", "id2", 50.0),
                new TransferFunds("id2", "id1", 50.0));

        inParallel(create1, create2).andThen(add).andThen(transfer);

        Result<SagaError, Saga<SpecificRecord>> sagaBuildResult = builder.build();
        assertThat(sagaBuildResult.isSuccess()).isEqualTo(true);

        return sagaBuildResult.getOrElse(null);
    }

    static <A> void validataSaga(Saga<A> actual, Saga<A>  expected) {
        assertThat(actual.status).isEqualTo(expected.status);
        assertThat(actual.sequence.getSeq()).isEqualTo(expected.sequence.getSeq());
        assertThat(actual.sagaId).isEqualTo(expected.sagaId);
        assertThat(actual.actions).hasSameSizeAs(expected.actions);
        validateErrors(actual.sagaError, expected.sagaError);
        actual.actions.forEach((k, a) -> {
            SagaAction<A> e = expected.actions.get(k);
            assertThat(a).isEqualToIgnoringGivenFields(e, "command", "undoCommand");
            assertThat(e.command.toString()).isEqualTo(a.command.toString());
            assertThat(e.undoCommand.toString()).isEqualTo(a.undoCommand.toString());
        });
    }

    static <A> void validateErrors(List<SagaError> actual, List<SagaError> expected) {
        assertThat(actual.size()).isEqualTo(expected.size());
        for (int i = 0; i < actual.size(); i++) {
            SagaError ae = actual.get(i);
            SagaError ee = actual.get(i);
            assertThat(ae.getMessage()).isEqualTo(ee.getMessage());
            assertThat(ae.getReason()).isEqualTo(ee.getReason());
        }
    }
}
