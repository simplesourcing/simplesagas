package io.simplesource.saga.saga.app;

import io.simplesource.kafka.internal.util.Tuple2;
import io.simplesource.saga.model.messages.*;
import io.simplesource.saga.model.saga.*;
import org.apache.kafka.streams.kstream.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


final class SagaStream {
    private static Logger logger = LoggerFactory.getLogger(SagaStream.class);

    static <K, V> ForeachAction<K, V> logValues(String prefix) {
        return (k, v) -> logger.info("{}: {}={}", prefix, k.toString().substring(0, 6), v.toString());
    }

    static <A> void addSubTopology(SagaContext<A> ctx,
                                   SagaTopologyBuilder.DelayedRetryPublisher<A> retryPublisher,
                                   KStream<SagaId, SagaRequest<A>> sagaRequestStream,
                                   KStream<SagaId, SagaStateTransition<A>> stateTransitionStream,
                                   KStream<SagaId, Saga<A>> stateStream,
                                   KStream<SagaId, ActionResponse<A>> actionResponseStream) {

        // create the state table from the state stream
        KTable<SagaId, Saga<A>> stateTable = createStateTable(ctx, stateStream);

        // validate saga requests
        Tuple2<KStream<SagaId, SagaRequest<A>>, KStream<SagaId, SagaResponse>> vrIr =
                SetupStream.validateSagaRequests(ctx, sagaRequestStream);

        // setup saga initial state
        KStream<SagaId, SagaStateTransition<A>> inputStateTransitions = SetupStream.addInitialState(ctx, vrIr.v1(), stateTable);

        // get actions to submit and handle action responses
        Tuple2<KStream<SagaId, SagaStateTransition<A>>, KStream<SagaId, ActionRequest<A>>> rtAr = ActionStream.getNextActions(stateStream);
        KStream<SagaId, SagaStateTransition<A>> responseTransitions = ActionStream.handleActionResponses(ctx, actionResponseStream, stateTable);

        // apply all saga state transitions
        KStream<SagaId, Saga<A>> sagaState = TransitionStream.applyStateTransitions(ctx, retryPublisher, stateTransitionStream);

        // generate saga responses and state transitions
        Tuple2<KStream<SagaId, SagaStateTransition<A>>, KStream<SagaId, SagaResponse>> stSr = ResponseStream.addSagaResponse(stateStream);

        // publish to all the value topics
        SagaProducer.publishActionRequests(ctx, rtAr.v2());
        SagaProducer.publishSagaStateTransitions(ctx, inputStateTransitions);
        SagaProducer.publishSagaStateTransitions(ctx, rtAr.v1());
        SagaProducer.publishSagaStateTransitions(ctx, responseTransitions);
        SagaProducer.publishSagaStateTransitions(ctx, stSr.v1());
        SagaProducer.publishSagaState(ctx, sagaState);
        SagaProducer.publishSagaResponses(ctx, vrIr.v2());
        SagaProducer.publishSagaResponses(ctx, stSr.v2());
    }

    private static <A> KTable<SagaId, Saga<A>> createStateTable(SagaContext<A> ctx, KStream<SagaId, Saga<A>> stateStream) {
        return stateStream.groupByKey(Grouped.with(ctx.sSerdes.sagaId(), ctx.sSerdes.state())).reduce(
                (s1, s2) -> (s1.sequence.getSeq() > s2.sequence.getSeq()) ? s1 : s2,
                Materialized.with(ctx.sSerdes.sagaId(), ctx.sSerdes.state()));
    }

}
