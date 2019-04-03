package io.simplesource.saga.action.internal;

import java.util.Properties;
import java.util.function.Function;

import io.simplesource.saga.action.async.AsyncContext;
import io.simplesource.saga.model.messages.ActionRequest;
import io.simplesource.saga.model.serdes.TopicSerdes;
import io.simplesource.saga.action.async.AsyncSpec;
import io.simplesource.saga.model.messages.ActionResponse;
import io.simplesource.saga.model.saga.SagaId;
import io.simplesource.saga.model.specs.ActionSpec;
import io.simplesource.saga.shared.kafka.ConsumerRunner;
import io.simplesource.saga.shared.topics.TopicTypes;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

final class AsyncProcessor {
    private static final boolean useTransactions = false;

    static <K, V> ProducerRecord<byte[], byte[]> toBytes(
            ProducerRecord<K, V> record,
            Serde<K> kSerde,
            Serde<V> vSerde) {
        String topicName = record.topic();
        byte[] kb = kSerde.serializer().serialize(topicName, record.key());
        byte[] kv = vSerde.serializer().serialize(topicName, record.value());
        return new ProducerRecord<>(topicName, kb, kv);
    }

    static <A, D, K, O, R> AsyncPipe apply(AsyncContext<A, D, K, O, R> asyncContext, Properties config) {
        AsyncSpec<A, D, K, O, R> asyncSpec = asyncContext.asyncSpec;
        ActionSpec<A> actionSpec = asyncContext.actionSpec;

        Function<Properties, Properties> copyProperties = properties -> {
            Properties newProps = new Properties();
            properties.forEach((key, value) -> newProps.setProperty(key.toString(), value.toString()));
            return newProps;
        };

        Properties consumerConfig = copyProperties.apply(config);
        //consumerConfig.putAll(spec.properties)
        consumerConfig.setProperty(ConsumerConfig.GROUP_ID_CONFIG,
                asyncSpec.groupId + "_async_consumer_" + asyncSpec.actionType);
        // TODO: don't hard code this
        consumerConfig.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        consumerConfig.setProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        consumerConfig.setProperty(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");

        Properties producerProps = copyProperties.apply(config);
        if (useTransactions)
            producerProps.setProperty(ProducerConfig.TRANSACTIONAL_ID_CONFIG, asyncSpec.groupId + "_async_producer");
        producerProps.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");

        KafkaProducer<byte[], byte[]> producer =
                new KafkaProducer<>(producerProps,
                        Serdes.ByteArray().serializer(),
                        Serdes.ByteArray().serializer());
        if (useTransactions)
            producer.initTransactions();

        AsyncPublisher<SagaId, ActionResponse<A>> responsePublisher = new AsyncKafkaPublisher<>(producer, asyncContext.actionSpec.serdes.sagaId(), asyncContext.actionSpec.serdes.response());
        Function<TopicSerdes<K, R>, AsyncPublisher<K, R>> outputPublisher = serdes -> new AsyncKafkaPublisher<>(producer, serdes.key, serdes.value);

        ConsumerRunner<SagaId, ActionRequest<A>> runner = new ConsumerRunner<>(
                consumerConfig,
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