package io.simplesource.saga.serialization.avro;

import io.simplesource.api.CommandId;
import io.simplesource.data.NonEmptyList;
import io.simplesource.data.Result;
import io.simplesource.saga.model.action.ActionCommand;
import io.simplesource.saga.model.messages.UndoCommand;
import io.simplesource.saga.model.saga.SagaError;
import io.simplesource.saga.serialization.avro.generated.AvroActionCommand;
import io.simplesource.saga.serialization.avro.generated.AvroActionUndoCommand;
import io.simplesource.saga.serialization.avro.generated.AvroActionUndoCommandOption;
import io.simplesource.saga.serialization.avro.generated.AvroSagaError;
import org.apache.avro.generic.GenericArray;
import org.apache.kafka.common.serialization.Serde;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class SagaSerdeUtils {
    public static String PAYLOAD_TOPIC_SUFFIX = "payload";

    static <R, T> Result<SagaError, T> sagaResultFromAvro(Object aRes, Function<R, T> successTransformer) {
        // TODO: remove the casting
        Result<SagaError, T> result;
        if (aRes instanceof GenericArray) {
            GenericArray<Object> aResArray = (GenericArray) aRes;
            Stream<AvroSagaError> avroErrors = aResArray.stream()
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
        ByteBuffer serializedPayload = serializeCommand(payloadSerde, payloadTopic, ac.command, ac.actionType);
        return AvroActionCommand
                .newBuilder()
                .setCommandId(ac.commandId.id().toString())
                .setCommand(serializedPayload)
                .setActionType(ac.actionType)
                .build();
    }

    static <A> UndoCommand<A> actionUndoCommandFromAvro(Serde<A> payloadSerde, String payloadTopic, AvroActionUndoCommand auc) {
        if (auc == null) return null;
        A command = deserializeCommand(payloadSerde, payloadTopic, auc.getCommand(), auc.getActionType());
        return UndoCommand.of(command, auc.getActionType());
    }

    static <A> AvroActionUndoCommand actionUndoCommandToAvro(Serde<A> payloadSerde, String payloadTopic, UndoCommand<A> ac) {
        ByteBuffer serializedPayload = serializeCommand(payloadSerde, payloadTopic, ac.command, ac.actionType);
        return AvroActionUndoCommand
                .newBuilder()
                .setCommand(serializedPayload)
                .setActionType(ac.actionType)
                .build();
    }

    static <A> Optional<UndoCommand<A>> optionalActionUndoCommandFromAvro(Serde<A> payloadSerde, String payloadTopic, AvroActionUndoCommandOption aucOption) {
        AvroActionUndoCommand undoCommandOpt = aucOption.getUndoCommand();
        return Optional.ofNullable(SagaSerdeUtils.actionUndoCommandFromAvro(payloadSerde, payloadTopic, undoCommandOpt));
    }

    static <A> AvroActionUndoCommandOption optionalActionUndoCommandFromAvro(Serde<A> payloadSerde, String payloadTopic, Optional<UndoCommand<A>> optUac) {
        return new AvroActionUndoCommandOption(optUac.map(uc -> SagaSerdeUtils.actionUndoCommandToAvro(payloadSerde, payloadTopic, uc)).orElse(null));
    }

    static <A> ActionCommand<A> actionCommandFromAvro(Serde<A> payloadSerde, String payloadTopic, AvroActionCommand ac) {
        if (ac == null) return null;
        A command = deserializeCommand(payloadSerde, payloadTopic, ac.getCommand(), ac.getActionType());
        return ActionCommand.of(CommandId.of(UUID.fromString(ac.getCommandId())), command, ac.getActionType());
    }

    public static <A> ByteBuffer serializeCommand(Serde<A> payloadSerde, String payloadTopic, A command, String actionType) {
        return ByteBuffer.wrap(payloadSerde.serializer().serialize(getSubjectName(payloadTopic, actionType), command));
    }

    public static <A> A deserializeCommand(Serde<A> payloadSerde, String payloadTopic, ByteBuffer commandBytes, String actionType) {
        return payloadSerde.deserializer().deserialize(getSubjectName(payloadTopic, actionType), commandBytes.array());
    }

    static String getSubjectName(String topic, String actionType) {
        String normalisedActionType = actionType.toLowerCase().replace(" ", "_");
        return String.format("%s-%s-%s", topic, normalisedActionType, PAYLOAD_TOPIC_SUFFIX);
    }
}
