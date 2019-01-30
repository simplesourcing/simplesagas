package io.simplesource.saga.serialization.avro;

import io.simplesource.data.NonEmptyList;
import io.simplesource.data.Result;
import io.simplesource.data.Sequence;
import io.simplesource.saga.model.action.ActionCommand;
import io.simplesource.saga.model.action.ActionStatus;
import io.simplesource.saga.model.action.SagaAction;
import io.simplesource.saga.model.saga.Saga;
import io.simplesource.saga.model.saga.SagaError;
import io.simplesource.saga.model.saga.SagaStatus;
import io.simplesource.saga.serialization.avro.generated.AvroActionCommand;
import io.simplesource.saga.serialization.avro.generated.AvroSaga;
import io.simplesource.saga.serialization.avro.generated.AvroSagaAction;
import io.simplesource.saga.serialization.avro.generated.AvroSagaError;
import org.apache.avro.generic.GenericArray;
import org.apache.kafka.common.serialization.Serde;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class SagaSerdeUtils {

    static <R, T> Result<SagaError, T> sagaResultFromAvro(Object aRes, Function<R, T> successTransformer) {
        // TODO: remove the casting
        Result<SagaError, T> result;
        if (aRes instanceof GenericArray) {
            GenericArray<Object> v = (GenericArray) aRes;
            Stream<AvroSagaError> avroErrors = v.stream()
                    .map(x -> (AvroSagaError) x)
                    .filter(Objects::nonNull);
            result = sagaErrorFromAvro(avroErrors);
        } else {
            R tRes = (R) aRes;
            if (tRes != null) {
                result = Result.success(successTransformer.apply(tRes));
            } else {
                result = Result.failure(SagaError.of(SagaError.Reason.InternalError, "Serialization error"));
            }
        }
        return result;
    }

    static <T> Result<SagaError, T> sagaErrorFromAvro(Stream<AvroSagaError> aRes) {
        return Result.failure(NonEmptyList.fromList(
                aRes.map(ae -> SagaError.of(SagaError.Reason.valueOf(ae.getReason()), ae.getMessage()))
                        .collect(Collectors.toList()))
                .orElse(NonEmptyList.of(
                        SagaError.of(SagaError.Reason.InternalError, "Serialization error"))));
    }

    static AvroSagaError sagaErrorToAvro(SagaError e) {
        return new AvroSagaError(
                e.getReason().toString(),
                e.getMessage());
    }

    static SagaError sagaErrorFromAvro(AvroSagaError e) {
        if (e == null) return null;
        return SagaError.of(
                SagaError.Reason.valueOf(e.getReason()),
                e.getMessage());
    }

    static List<AvroSagaError> sagaErrorListToAvro(NonEmptyList<SagaError> es) {
        return es.map(SagaSerdeUtils::sagaErrorToAvro).toList();
    }

    static List<AvroSagaError> sagaErrorListToAvro(List<SagaError> es) {
        return es.stream()
                .map(SagaSerdeUtils::sagaErrorToAvro)
                .collect(Collectors.toList());
    }

    static List<SagaError> sagaErrorListFromAvro(List<AvroSagaError> es) {
        return es.stream()
                .map(SagaSerdeUtils::sagaErrorFromAvro)
                .collect(Collectors.toList());
    }

    static <A> AvroActionCommand actionCommandToAvro(Serde<A> payloadSerde, String payloadTopic, ActionCommand<A> ac) {
        byte[] serializedPayload = payloadSerde.serializer().serialize(payloadTopic, ac.command);
        return AvroActionCommand
                .newBuilder()
                .setCommandId(ac.commandId.toString())
                .setCommand(ByteBuffer.wrap(serializedPayload))
                .build();
    }

    static <A> ActionCommand<A> actionCommandFromAvro(Serde<A> payloadSerde, String payloadTopic, AvroActionCommand ac) {
        if (ac == null) return null;
        A command = payloadSerde.deserializer().deserialize(payloadTopic, ac.getCommand().array());
        return new ActionCommand<>(UUID.fromString(ac.getCommandId()), command);
    }
}
