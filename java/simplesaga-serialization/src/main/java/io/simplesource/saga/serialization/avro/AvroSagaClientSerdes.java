package io.simplesource.saga.serialization.avro;

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.simplesource.data.Sequence;
import io.simplesource.saga.model.action.ActionStatus;
import io.simplesource.saga.model.action.SagaAction;
import io.simplesource.saga.model.messages.SagaRequest;
import io.simplesource.saga.model.messages.SagaResponse;
import io.simplesource.saga.model.saga.Saga;
import io.simplesource.saga.model.saga.SagaStatus;
import io.simplesource.saga.model.serdes.SagaClientSerdes;
import io.simplesource.saga.serialization.avro.generated.*;
import io.simplesource.saga.serialization.utils.SerdeUtils;
import org.apache.kafka.common.serialization.Serde;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

public class AvroSagaClientSerdes<A> implements SagaClientSerdes<A> {

    final Serde<A> payloadSerde;
    private final Serde<AvroSagaId> avroSagaIdSerde;
    private final Serde<AvroSagaRequest> avroSagaRequestSerde;
    private final Serde<AvroSagaResponse> avroSagaResponseSerde;

    AvroSagaClientSerdes(
            final Serde<A> payloadSerde,
            final String schemaRegistryUrl,
            final boolean useMockSchemaRegistry) {
        this.payloadSerde = payloadSerde;

        SchemaRegistryClient regClient = useMockSchemaRegistry ? new MockSchemaRegistryClient() : null;
        avroSagaIdSerde = SpecificSerdeUtils.specificAvroSerde(schemaRegistryUrl, true, regClient);
        avroSagaRequestSerde = SpecificSerdeUtils.specificAvroSerde(schemaRegistryUrl, false, regClient);
        avroSagaResponseSerde = SpecificSerdeUtils.specificAvroSerde(schemaRegistryUrl, false, regClient);
    }

    @Override
    public Serde<UUID> uuid() {
        return SerdeUtils.iMap(avroSagaIdSerde, id -> new AvroSagaId(id.toString()), aid -> UUID.fromString(aid.getId()));
    }

    @Override
    public Serde<SagaRequest<A>> request() {

        return SerdeUtils.iMap(avroSagaRequestSerde, (topic, sr) -> {
            Saga<A> s = sr.initialState;
            AvroSaga avroSaga = sagaToAvro(topic, s);
            return AvroSagaRequest.newBuilder()
                    .setInitialState(avroSaga)
                    .setSagaId(sr.sagaId.toString())
                    .build();
        }, (topic, asr) -> {
            AvroSaga as = asr.getInitialState();
            Saga<A> saga = sagaFromAvro(topic, as);
            return new SagaRequest<>(UUID.fromString(asr.getSagaId()), saga);
        });
    }

    @Override
    public Serde<SagaResponse> response() {
        return SerdeUtils.iMap(avroSagaResponseSerde,
                r -> AvroSagaResponse.newBuilder()
                        .setSagaId(r.sagaId.toString())
                        .setResult(r.result.fold(
                                es -> es.map(e ->
                                        new AvroSagaError(
                                                e.getReason().toString(),
                                                e.getMessage()))
                                        .toList(),
                                Sequence::getSeq))
                        .build(),
                ar -> new SagaResponse(
                        UUID.fromString(ar.getSagaId()),
                        SagaSerdeUtils.<Long, Sequence>sagaResultFromAvro(ar.getResult(), x -> Sequence.position(x))));

    }

    protected Saga<A> sagaFromAvro(String topic, AvroSaga as) {
        Map<String, AvroSagaAction> aActions = as.getActions();
        Map<UUID, SagaAction<A>> actions = new HashMap<>();
        aActions.forEach((id, aa) -> {
            UUID actionId = UUID.fromString(aa.getActionId());
            SagaAction<A> action = new SagaAction<>(
                    actionId,
                    aa.getActionType(),
                    SagaSerdeUtils.actionCommandFromAvro(payloadSerde, topic, aa.getActionType(), aa.getActionCommand()),
                    Optional.ofNullable(SagaSerdeUtils.actionCommandFromAvro(payloadSerde, topic, aa.getActionType() + "-undo", aa.getUndoCommand())),
                    aa.getDependencies().stream().map(UUID::fromString).collect(Collectors.toSet()),
                    ActionStatus.valueOf(aa.getActionStatus()),
                    SagaSerdeUtils.sagaErrorListFromAvro(aa.getActionErrors()));
            actions.put(actionId, action);
        });

        return Saga.of(
                UUID.fromString(as.getSagaId()),
                actions,
                SagaStatus.valueOf(as.getSagaStatus()),

                Sequence.position(as.getSequence()));
    }

    protected AvroSaga sagaToAvro(String topic, Saga<A> s) {
        Map<String, AvroSagaAction> avroActions = new HashMap<>();
        s.actions.forEach((id, act) -> {
            String actionId = id.toString();
            AvroSagaAction avroSagaAction = AvroSagaAction.newBuilder()
                    .setActionId(actionId)
                    .setActionErrors(SagaSerdeUtils.sagaErrorListToAvro(act.error))
                    .setActionCommand(SagaSerdeUtils.actionCommandToAvro(
                            payloadSerde,
                            topic,
                            act.actionType,
                            act.command))
                    .setUndoCommand(act.undoCommand.map(uc -> SagaSerdeUtils.actionCommandToAvro(
                            payloadSerde,
                            topic,
                            act.actionType + "-undo",
                            uc)).orElse(null))
                    .setActionStatus(act.status.toString())
                    .setActionType(act.actionType)
                    .setDependencies(act.dependencies
                            .stream()
                            .map(UUID::toString)
                            .collect(Collectors.toList()))
                    .build();
            avroActions.put(actionId, avroSagaAction);
        });

        return AvroSaga
                .newBuilder()
                .setSagaId(s.sagaId.toString())
                .setSagaStatus(s.status.toString())
                .setSagaErrors(SagaSerdeUtils.sagaErrorListToAvro(s.sagaError))
                .setActions(avroActions)
                .setSequence(s.sequence.getSeq())
                .build();
    }
}

