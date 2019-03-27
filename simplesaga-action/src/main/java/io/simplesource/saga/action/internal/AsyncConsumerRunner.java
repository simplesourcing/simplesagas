package io.simplesource.saga.action.internal;

import io.simplesource.saga.action.async.*;
import io.simplesource.saga.model.messages.ActionRequest;
import io.simplesource.saga.model.messages.ActionResponse;
import io.simplesource.saga.model.saga.SagaId;
import io.simplesource.saga.model.serdes.TopicSerdes;
import io.simplesource.saga.model.specs.ActionSpec;
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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Function;

final class AsyncConsumerRunner<A, D, K, O, R> implements Runnable {

    private final AsyncSpec<A, D, K, O, R> asyncSpec;
    private final ActionSpec<A> actionSpec;
    private final Consumer<Boolean> onClose;
    private final AtomicBoolean closed;
    private Optional<KafkaConsumer<SagaId, ActionRequest<A>>> consumer = Optional.empty();
    private final Logger logger = LoggerFactory.getLogger(AsyncConsumerRunner.class);
    private final Properties consumerConfig;
    private final AsyncContext<A, D, K, O, R> asyncContext;
    private final AsyncPublisher<SagaId, ActionResponse> responsePublisher;
    private final Function<TopicSerdes<K, R>, AsyncPublisher<K, R>> outputPublisher;

    AsyncConsumerRunner(
            AsyncContext<A, D, K, O, R> asyncContext,
            Properties consumerConfig,
            AsyncPublisher<SagaId, ActionResponse> responsePublisher,
            Function<TopicSerdes<K, R>, AsyncPublisher<K, R>> outputPublisher,
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

        KafkaConsumer<SagaId, ActionRequest<A>> consumer = new KafkaConsumer<>(consumerConfig,
                actionSpec.serdes.sagaId().deserializer(),
                actionSpec.serdes.request().deserializer());
        consumer.subscribe(Collections.singletonList(asyncContext.actionTopicNamer.apply(TopicTypes.ActionTopic.ACTION_REQUEST_UNPROCESSED)));

        this.consumer = Optional.of(consumer);

        try {
            while (!closed.get()) {
                ConsumerRecords<SagaId, ActionRequest<A>> records = consumer.poll(Duration.ofMillis(100L));
                for (ConsumerRecord<SagaId, ActionRequest<A>> x : records) {
                    SagaId sagaId = x.key();
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

