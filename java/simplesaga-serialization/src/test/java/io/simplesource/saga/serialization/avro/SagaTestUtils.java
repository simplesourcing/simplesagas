package io.simplesource.saga.serialization.avro;

import io.simplesource.data.Result;
import io.simplesource.saga.model.action.ActionCommand;
import io.simplesource.saga.model.saga.Saga;
import io.simplesource.saga.model.saga.SagaError;
import io.simplesource.saga.saga.dsl.SagaDsl;
import io.simplesource.saga.serialization.avro.generated.test.AddFunds;
import io.simplesource.saga.serialization.avro.generated.test.CreateAccount;
import io.simplesource.saga.serialization.avro.generated.test.TransferFunds;
import org.apache.avro.generic.GenericRecord;

import java.util.UUID;
import java.util.function.BiFunction;
import java.util.function.Function;

import static io.simplesource.saga.saga.dsl.SagaDsl.inParallel;
import static org.assertj.core.api.Assertions.assertThat;

public class SagaTestUtils {

    static Saga<GenericRecord> getTestSaga() {
        SagaDsl.SagaBuilder<GenericRecord> builder = SagaDsl.SagaBuilder.create();

        Function<GenericRecord, SagaDsl.SubSaga<GenericRecord>> addAction = command ->
                builder.addAction(
                        UUID.randomUUID(),
                        "actionType",
                        new ActionCommand<>(UUID.randomUUID(), command));

        BiFunction<GenericRecord, GenericRecord, SagaDsl.SubSaga<GenericRecord>> addActionWithUndo = (command, undo) ->
                builder.addAction(
                        UUID.randomUUID(),
                        "actionType",
                        new ActionCommand<>(UUID.randomUUID(), command),
                        new ActionCommand<>(UUID.randomUUID(), undo));

        SagaDsl.SubSaga<GenericRecord> create1 = addAction.apply(new CreateAccount("id1", "User 1"));
        SagaDsl.SubSaga<GenericRecord> create2 = addAction.apply(new CreateAccount("id2", "User 2"));

        SagaDsl.SubSaga<GenericRecord> add = addActionWithUndo.apply(
                new AddFunds("id1", 1000.0),
                new AddFunds("id1", -1000.0));
        SagaDsl.SubSaga<GenericRecord> transfer = addActionWithUndo.apply(
                new TransferFunds("id1", "id2", 50.0),
                new TransferFunds("id2", "id1", 50.0));

        inParallel(create1, create2).andThen(add).andThen(transfer);

        Result<SagaError, Saga<GenericRecord>> sagaBuildResult = builder.build();
        assertThat(sagaBuildResult.isSuccess()).isEqualTo(true);

        return sagaBuildResult.getOrElse(null);
    }

}
