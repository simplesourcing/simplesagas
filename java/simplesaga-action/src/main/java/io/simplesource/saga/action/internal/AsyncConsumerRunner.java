package io.simplesource.saga.action.internal;

import io.simplesource.saga.action.async.*;
import io.simplesource.saga.model.messages.ActionRequest;
import io.simplesource.saga.model.messages.ActionResponse;
import io.simplesource.saga.model.specs.ActionProcessorSpec;
import io.simplesource.saga.shared.topics.TopicTypes;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Function;

class AsyncConsumerRunner<A, D, K, O, R> implements Runnable {

    private final AsyncSpec<A, D, K, O, R> asyncSpec;
    private final ActionProcessorSpec<A> actionSpec;
    private final Consumer<Boolean> onClose;
    private final AtomicBoolean closed;
    private Optional<KafkaConsumer<UUID, ActionRequest<A>>> consumer = Optional.empty();
    private final Logger logger = LoggerFactory.getLogger(AsyncConsumerRunner.class);
    private final Properties consumerConfig;
    private final AsyncContext<A, D, K, O, R> asyncContext;
    private final AsyncPublisher<UUID, ActionResponse> responsePublisher;
    private final Function<AsyncSerdes<K, R>, AsyncPublisher<K, R>> outputPublisher;

    AsyncConsumerRunner(
            AsyncContext<A, D, K, O, R> asyncContext,
            Properties consumerConfig,
            AsyncPublisher<UUID, ActionResponse> responsePublisher,
            Function<AsyncSerdes<K, R>, AsyncPublisher<K, R>> outputPublisher,
            Consumer<Boolean> onClose) {
        asyncSpec = asyncContext.asyncSpec;
        actionSpec = asyncContext.actionSpec;
        this.responsePublisher = responsePublisher;
        this.outputPublisher = outputPublisher;
        this.onClose = onClose;
        closed = new AtomicBoolean(false);
        this.asyncContext = asyncContext;
        this.consumerConfig = consumerConfig;
    }

    @Override
    public void run() {

        KafkaConsumer<UUID, ActionRequest<A>> consumer = new KafkaConsumer<>(consumerConfig,
                actionSpec.serdes.uuid().deserializer(),
                actionSpec.serdes.request().deserializer());
        consumer.subscribe(Collections.singletonList(asyncContext.actionTopicNamer.apply(TopicTypes.ActionTopic.requestUnprocessed)));

        this.consumer = Optional.of(consumer);

        try {
            while (!closed.get()) {
                ConsumerRecords<UUID, ActionRequest<A>> records = consumer.poll(Duration.ofMillis(100L));
                for (ConsumerRecord<UUID, ActionRequest<A>> x : records) {
                    UUID sagaId = x.key();
                    ActionRequest<A> request = x.value();
                    if (request.actionType.equals(asyncSpec.actionType)) {
                        AsyncActionProcessor.processRecord(asyncContext, sagaId, request, responsePublisher, outputPublisher);
                    }
                }
            }
        } catch (WakeupException e) {
            if (!closed.get()) throw e;
        } finally {
            logger.info("Closing consumer and producer");
            consumer.commitSync();
            consumer.close();
            this.onClose.accept(true);
        }
    }

    void close() {
        closed.set(true);
        consumer.ifPresent(KafkaConsumer::wakeup);
    }
}

