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
import java.util.UUID;
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
    public Serde<SagaStateTransition> transition() {
        return SerdeUtils.iMap(avroSagaTransitionSerde, (topic, at) -> {
            Object transition = at.cata(
                    initial -> new AvroSagaTransitionInitial(sagaToAvro(topic, (Saga<A>)initial.sagaState)),
                    AvroSagaSerdes::actionStatusChangeToAvro,
                    sagaChange -> AvroSagaTransitionSagaStatusChange.newBuilder()
                            .setSagaId(sagaChange.sagaId.toString())
                            .setSagaStatus(sagaChange.sagaStatus.toString())
                            .setSagaErrors(SagaSerdeUtils.sagaErrorListToAvro(sagaChange.sagaErrors))
                            .build(),
                    changeList -> new AvroSagaTransitionList(
                            changeList.actions
                                    .stream()
                                    .map(AvroSagaSerdes::actionStatusChangeToAvro)
                                    .collect(Collectors.toList())));
            return new AvroSagaTransition(transition);
        }, (topic, at) -> {
            Object t = at.getTransition();
            if (t instanceof AvroSagaTransitionInitial) {
                return new SagaStateTransition.SetInitialState<>(sagaFromAvro(topic, ((AvroSagaTransitionInitial)t).getSagaState()));
            }
            if (t instanceof AvroSagaTransitionActionStatusChange) {
                return actionStatusChangeFromAvro((AvroSagaTransitionActionStatusChange) t);
            }
            if (t instanceof AvroSagaTransitionSagaStatusChange) {
                AvroSagaTransitionSagaStatusChange st = (AvroSagaTransitionSagaStatusChange) t;
                return new SagaStateTransition.SagaStatusChanged(
                        SagaId.fromString(st.getSagaId()),
                        SagaStatus.valueOf(st.getSagaStatus()),
                        SagaSerdeUtils.sagaErrorListFromAvro(st.getSagaErrors()));
            }
            if (t instanceof AvroSagaTransitionList) {
                AvroSagaTransitionList l = (AvroSagaTransitionList) t;
                List<SagaStateTransition.SagaActionStatusChanged> actions =
                        l.getActionChanges().stream().map(AvroSagaSerdes::actionStatusChangeFromAvro).collect(Collectors.toList());
                return new SagaStateTransition.TransitionList(actions);
            }
            throw new RuntimeException("Unexpected exception. Avro failed to validate SagaStateTransition union type.");
        });
    }

    private static AvroSagaTransitionActionStatusChange actionStatusChangeToAvro(SagaStateTransition.SagaActionStatusChanged actionChange) {
        return AvroSagaTransitionActionStatusChange.newBuilder()
                .setSagaId(actionChange.sagaId.toString())
                .setActionId(actionChange.actionId.toString())
                .setActionStatus(actionChange.actionStatus.toString())
                .setActionErrors(SagaSerdeUtils.sagaErrorListToAvro(actionChange.actionErrors))
                .build();
    }

    private static SagaStateTransition.SagaActionStatusChanged actionStatusChangeFromAvro(AvroSagaTransitionActionStatusChange actionChange) {
        return new SagaStateTransition.SagaActionStatusChanged(
                SagaId.fromString(actionChange.getSagaId()),
                ActionId.fromString(actionChange.getActionId()),
                ActionStatus.valueOf(actionChange.getActionStatus()),
                SagaSerdeUtils.sagaErrorListFromAvro(actionChange.getActionErrors()));
    }
}
