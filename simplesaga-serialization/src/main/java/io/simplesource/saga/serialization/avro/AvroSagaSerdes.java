package io.simplesource.saga.serialization.avro;

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.simplesource.saga.model.action.ActionId;
import io.simplesource.saga.model.action.ActionStatus;
import io.simplesource.saga.model.messages.SagaStateTransition;
import io.simplesource.saga.model.saga.Saga;
import io.simplesource.saga.model.saga.SagaId;
import io.simplesource.saga.model.saga.SagaStatus;
import io.simplesource.saga.model.serdes.SagaSerdes;
import io.simplesource.saga.serialization.avro.generated.*;
import io.simplesource.saga.serialization.utils.SerdeUtils;
import org.apache.kafka.common.serialization.Serde;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class AvroSagaSerdes<A> extends AvroSagaClientSerdes<A> implements SagaSerdes<A> {

    private final Serde<AvroSaga> avroSagaSerde;
    private final Serde<AvroSagaTransition> avroSagaTransitionSerde;

    AvroSagaSerdes(
            final Serde<A> payloadSerde,
            final String schemaRegistryUrl,
            final boolean useMockSchemaRegistry) {
        super(payloadSerde, schemaRegistryUrl, useMockSchemaRegistry);

        SchemaRegistryClient regClient = useMockSchemaRegistry ? new MockSchemaRegistryClient() : null;
        avroSagaSerde = SpecificSerdeUtils.specificAvroSerde(schemaRegistryUrl, false, regClient);
        avroSagaTransitionSerde = SpecificSerdeUtils.specificAvroSerde(schemaRegistryUrl, false, regClient);
    }

    @Override
    public Serde<Saga<A>> state() {
        return SerdeUtils.iMap(avroSagaSerde, this::sagaToAvro, this::sagaFromAvro);
    }

    @Override
    public Serde<SagaStateTransition<A>> transition() {
        return SerdeUtils.iMap(avroSagaTransitionSerde, (topic, at) -> {
            Object transition = at.cata(
                    initial -> new AvroSagaTransitionInitial(sagaToAvro(topic, (Saga<A>)initial.sagaState)),
                    ac -> actionStatusChangeToAvro(ac, topic),
                    sagaChange -> AvroSagaTransitionSagaStatusChange.<A>newBuilder()
                            .setSagaId(sagaChange.sagaId.toString())
                            .setSagaStatus(sagaChange.sagaStatus.toString())
                            .setSagaErrors(SagaSerdeUtils.sagaErrorListToAvro(sagaChange.sagaErrors))
                            .build(),
                    changeList -> new AvroSagaTransitionList(
                            changeList.actions
                                    .stream()
                                    .map(ac -> actionStatusChangeToAvro(ac, topic))
                                    .collect(Collectors.toList())));
            return new AvroSagaTransition(transition);
        }, (topic, at) -> {
            Object t = at.getTransition();
            if (t instanceof AvroSagaTransitionInitial) {
                return SagaStateTransition.SetInitialState.of(sagaFromAvro(topic, ((AvroSagaTransitionInitial)t).getSagaState()));
            }
            if (t instanceof AvroSagaTransitionActionStateChange) {
                return actionStatusChangeFromAvro((AvroSagaTransitionActionStateChange) t, topic);
            }
            if (t instanceof AvroSagaTransitionSagaStatusChange) {
                AvroSagaTransitionSagaStatusChange st = (AvroSagaTransitionSagaStatusChange) t;
                return SagaStateTransition.SagaStatusChanged.of(
                        SagaId.fromString(st.getSagaId()),
                        SagaStatus.valueOf(st.getSagaStatus()),
                        SagaSerdeUtils.sagaErrorListFromAvro(st.getSagaErrors()));
            }
            if (t instanceof AvroSagaTransitionList) {
                AvroSagaTransitionList l = (AvroSagaTransitionList) t;
                List<SagaStateTransition.SagaActionStateChanged<A>> actions =
                        l.getActionChanges().stream().map(ac -> actionStatusChangeFromAvro(ac, topic)).collect(Collectors.toList());
                return SagaStateTransition.TransitionList.of(actions);
            }
            throw new RuntimeException("Unexpected exception. Avro failed to validate SagaStateTransition union type.");
        });
    }

    private AvroSagaTransitionActionStateChange actionStatusChangeToAvro(SagaStateTransition.SagaActionStateChanged<A> actionChange, String topic) {
        return AvroSagaTransitionActionStateChange.newBuilder()
                .setSagaId(actionChange.sagaId.toString())
                .setActionId(actionChange.actionId.toString())
                .setActionStatus(actionChange.actionStatus.toString())
                .setActionErrors(SagaSerdeUtils.sagaErrorListToAvro(actionChange.actionErrors))
                .setUndoCommand(actionChange.undoCommand.map(uc -> SagaSerdeUtils.actionUndoCommandToAvro(
                        payloadSerde,
                        topic,
                        uc)).orElse(null))
                .build();
    }

    private SagaStateTransition.SagaActionStateChanged<A> actionStatusChangeFromAvro(AvroSagaTransitionActionStateChange actionChange, String topic) {
        return SagaStateTransition.SagaActionStateChanged.of(
                SagaId.fromString(actionChange.getSagaId()),
                ActionId.fromString(actionChange.getActionId()),
                ActionStatus.valueOf(actionChange.getActionStatus()),
                SagaSerdeUtils.sagaErrorListFromAvro(actionChange.getActionErrors()),
                Optional.ofNullable(SagaSerdeUtils.actionUndoCommandFromAvro(payloadSerde, topic, actionChange.getUndoCommand())));
    }
}
