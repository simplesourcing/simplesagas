package io.simplesource.saga.saga.internal;

import io.simplesource.saga.model.action.ActionStatus;
import io.simplesource.saga.model.action.SagaAction;
import io.simplesource.saga.model.messages.SagaStateTransition;
import io.simplesource.saga.model.saga.Saga;
import io.simplesource.saga.model.saga.SagaId;
import io.simplesource.saga.model.serdes.SagaSerdes;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;

import java.util.HashMap;
import java.util.List;
import java.util.Optional;

final class TransitionStream {
    static <A> KStream<SagaId, Saga<A>> applyStateTransitions(SagaContext<A> ctx,
                                                              SagaTopologyBuilder.DelayedRetryPublisher<A> publisher,
                                                              KStream<SagaId, SagaStateTransition<A>> stateTransitionStream) {
        SagaSerdes<A> sSerdes = ctx.sSerdes;
        return stateTransitionStream
                .groupByKey(Grouped.with(sSerdes.sagaId(), sSerdes.transition()))
                .aggregate(() -> Saga.of(new HashMap<>()),
                        (k, t, s) -> {
                            ActionTransition.SagaWithRetry<A> sagaWithRetries = SagaTransition.applyTransition(t, s);
                            sendRetries(publisher, s, sagaWithRetries.retryActions);
                            return sagaWithRetries.saga;
                        },
                        Materialized.with(sSerdes.sagaId(), sSerdes.state()))
                .toStream();
    }

    private static <A> void sendRetries(SagaTopologyBuilder.DelayedRetryPublisher<A> publisher, Saga<A> s, List<ActionTransition.SagaWithRetry.Retry> retries) {

        retries.forEach(r -> {
            SagaAction<A> existing = s.actions.get(r.actionId);
            SagaStateTransition<A> transition =
                    SagaStateTransition.SagaActionStateChanged.of(
                            s.sagaId,
                            r.actionId,
                            ActionStatus.RetryCompleted,
                            existing.error,
                            Optional.empty(),
                            r.isUndo);

            publisher.send(r.actionType, r.retryCount, s.sagaId, transition);

        });
    }
}
