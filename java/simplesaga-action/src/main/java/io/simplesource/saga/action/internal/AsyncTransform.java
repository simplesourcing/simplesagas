package io.simplesource.saga.action.internal;

import java.util.Properties;
import java.util.function.Function;

import io.simplesource.saga.action.async.AsyncContext;
import io.simplesource.saga.action.async.AsyncSpec;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serde;

final class AsyncTransform {
    static final boolean useTransactions = false;

    public interface AsyncPipe {
        void close();
    }

    static <K, V> ProducerRecord<byte[], byte[]> toBytes(
            ProducerRecord<K, V> record,
            Serde<K> kSerde,
            Serde<V> vSerde) {
        String topicName = record.topic();
        byte[] kb = kSerde.serializer().serialize(topicName, record.key());
        byte[] kv = vSerde.serializer().serialize(topicName, record.value());
        return new ProducerRecord<>(topicName, kb, kv);
    }


    static <A, I, K, O, R> AsyncPipe async(AsyncContext<A, I, K, O, R> asyncContext, Properties config) {
        AsyncSpec<A, I, K, O, R> asyncSpec = asyncContext.asyncSpec;

        Function<Properties, Properties> copyProperties = properties -> {
            Properties newProps = new Properties();
            properties.forEach((key, value) -> newProps.setProperty(key.toString(), value.toString()));
            return newProps;
        };

        Properties consumerConfig = copyProperties.apply(config);
        //consumerConfig.putAll(spec.config)
        consumerConfig.setProperty(ConsumerConfig.GROUP_ID_CONFIG,
                asyncSpec.groupId + "_async_consumer_" + asyncSpec.actionType);
        // For now automatic - probably rather do this manually
        consumerConfig.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        consumerConfig.setProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        consumerConfig.setProperty(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");

        Properties producerProps = copyProperties.apply(config);
        if (useTransactions)
            producerProps.setProperty(ProducerConfig.TRANSACTIONAL_ID_CONFIG, asyncSpec.groupId + "_async_producer");
        producerProps.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");

        final ConsumerRunner runner = new ConsumerRunner<>(asyncContext, consumerConfig, producerProps);
        new Thread(runner).start();

        return runner::close;
    }

}