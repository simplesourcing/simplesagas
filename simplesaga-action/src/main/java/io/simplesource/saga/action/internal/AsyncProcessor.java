package io.simplesource.saga.action.internal;

import java.util.function.Function;

import io.simplesource.saga.action.async.AsyncContext;
import io.simplesource.saga.model.messages.ActionRequest;
import io.simplesource.saga.model.serdes.TopicSerdes;
import io.simplesource.saga.action.async.AsyncSpec;
import io.simplesource.saga.model.messages.ActionResponse;
import io.simplesource.saga.model.saga.SagaId;
import io.simplesource.saga.model.specs.ActionSpec;
import io.simplesource.saga.shared.kafka.*;
import io.simplesource.saga.shared.properties.PropertiesBuilder;
import io.simplesource.saga.shared.topics.TopicTypes;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.Serdes;

final class AsyncProcessor {
    private static final boolean useTransactions = false;

    static <A, D, K, O, R> AsyncPipe apply(AsyncContext<A, D, K, O, R> asyncContext, PropertiesBuilder.BuildSteps config) {
        AsyncSpec<A, D, K, O, R> asyncSpec = asyncContext.asyncSpec;
        ActionSpec<A> actionSpec = asyncContext.actionSpec;

        PropertiesBuilder.BuildSteps consumerProps = config
                .withNextStep(pb -> pb
                        .withProperty(ConsumerConfig.GROUP_ID_CONFIG, asyncSpec.groupId + "_async_consumer_" + asyncSpec.actionType));

        KafkaProducer<byte[], byte[]> producer =
                new KafkaProducer<>(config.build(PropertiesBuilder.Target.Producer),
                        Serdes.ByteArray().serializer(),
                        Serdes.ByteArray().serializer());
        if (useTransactions)
            producer.initTransactions();

        AsyncPublisher<SagaId, ActionResponse<A>> responsePublisher = new KafkaPublisher<>(
                producer,
                asyncContext.actionSpec.serdes.sagaId(),
                asyncContext.actionSpec.serdes.response()
        )::send;

        Function<TopicSerdes<K, R>, AsyncPublisher<K, R>> outputPublisher = serdes ->
                new KafkaPublisher<>(producer, serdes.key, serdes.value)::send;

        ConsumerRunner<SagaId, ActionRequest<A>> runner = new ConsumerRunner<>(
                consumerProps.build(PropertiesBuilder.Target.Consumer),
                (sagaId, request) ->
                        AsyncInvoker.processActionRequest(asyncContext, sagaId, request, responsePublisher, outputPublisher),
                actionSpec.serdes.sagaId(),
                actionSpec.serdes.request(),
                asyncContext.actionTopicNamer.apply(TopicTypes.ActionTopic.ACTION_REQUEST_UNPROCESSED), closed -> {
            producer.flush();
            producer.close();
        });
        new Thread(runner).start();

        return runner::close;
    }
}