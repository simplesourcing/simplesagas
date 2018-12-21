package io.simplesource.saga.action.async;

import io.simplesource.data.Result;
import io.simplesource.kafka.internal.util.Tuple2;
import io.simplesource.saga.model.messages.ActionRequest;
import io.simplesource.saga.model.messages.ActionResponse;
import io.simplesource.saga.model.saga.SagaError;
import io.simplesource.saga.model.specs.ActionProcessorSpec;
import io.simplesource.saga.shared.topics.TopicTypes;
import lombok.Value;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.Serdes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.function.Supplier;

import static io.simplesource.saga.action.async.AsyncTransform.*;

class ConsumerRunner<A, I, K, O, R> implements Runnable {

    private final AsyncSpec<A, I, K, O, R> asyncSpec;
    private final ActionProcessorSpec<A> actionSpec;
    private final AtomicBoolean closed;
    private Optional<KafkaConsumer<UUID, ActionRequest<A>>> consumer = Optional.empty();
    private final Logger logger = LoggerFactory.getLogger(ConsumerRunner.class);
    private final Properties consumerConfig;
    private final Properties producerProps;
    private final AsyncContext<A, I, K, O, R> asyncContext;
    private Optional<Result<Throwable, Optional<ResultGeneration<K, R>>>> y;

    ConsumerRunner(
            AsyncContext<A, I, K, O, R> asyncContext,
            Properties consumerConfig,
            Properties producerProps) {
        asyncSpec = asyncContext.asyncSpec;
        actionSpec = asyncContext.actionSpec;
        closed = new AtomicBoolean(false);
        this.asyncContext = asyncContext;
        this.consumerConfig = consumerConfig;
        this.producerProps = producerProps;
    }

    @Override
    public void run() {

        KafkaProducer<byte[], byte[]> producer =
                new KafkaProducer<>(producerProps,
                        Serdes.ByteArray().serializer(),
                        Serdes.ByteArray().serializer());
        if (useTransactions)
            producer.initTransactions();

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
                        processRecord(sagaId, request, producer);
                    }
                }
            }
        } catch (WakeupException e) {
            if (!closed.get()) throw e;
        } finally {
            logger.info("Closing consumer and producer");
            consumer.commitSync();
            consumer.close();
            producer.flush();
            producer.close();
        }
    }

    // execute lazy code and wraps exceptions in a result
    private static <X> Result<Throwable, X> tryPure(Supplier<X> xSupplier) {
        try {
            return Result.success(xSupplier.get());
        } catch (Throwable e) {
            return Result.failure(e);
        }
    }

    // evaluates code returning a Result that may throw an exception,
    // and turns it into a Result that is guaranteed not to throw 
    // (i.e. absorbs exceptions into the failure mode)
    private static <X> Result<Throwable, X> tryWrap(Supplier<Result<Throwable, X>> xSupplier) {
        try {
            return xSupplier.get();
        } catch (Throwable e) {
            return Result.failure(e);
        }
    }

    @Value
    private static class ResultGeneration<K, R> {
        final String topicName;
        final AsyncSerdes<K, R> outputSerdes;
        final R result;
    }

    private void publishActionResult(
            UUID sagaId,
            ActionRequest<A> request,
            KafkaProducer<byte[], byte[]> producer,
            Result<Throwable, ?> result) {

        Result<SagaError, Boolean> booleanResult = result.fold(es -> Result.failure(
                SagaError.of(SagaError.Reason.InternalError, es.head())),
                r -> Result.success(true));

        ActionResponse actionResponse = new ActionResponse(request.sagaId,
                request.actionId,
                request.actionCommand.commandId,
                booleanResult);

        ProducerRecord<UUID, ActionResponse> responseRecord = new ProducerRecord<>(
                asyncContext.actionTopicNamer.apply(TopicTypes.ActionTopic.response),
                sagaId,
                actionResponse);

        ProducerRecord<byte[], byte[]> byteRecord =
                AsyncTransform.toBytes(responseRecord, actionSpec.serdes.uuid(), actionSpec.serdes.response());
        producer.send(byteRecord);
    }

    private void processRecord(UUID sagaId, ActionRequest<A> request,
                               KafkaProducer<byte[], byte[]> producer) {

        Result<Throwable, Tuple2<I, K>> decodedWithKey = tryWrap(() ->
                asyncSpec.inputDecoder.apply(request.actionCommand.command))
                .flatMap(decoded ->
                        tryPure(() ->
                                asyncSpec.keyMapper.apply(decoded)).map(k -> Tuple2.of(decoded, k)));

        AtomicBoolean completed = new AtomicBoolean(false);
        Function<Tuple2<I, K>, Callback<O>> cpb = tuple -> result -> {
            if (completed.compareAndSet(false, true)) {
                Result<Throwable, Optional<ResultGeneration<K, R>>> resultWithOutput = tryWrap(() ->
                        result.flatMap(output -> {
                            Optional<Result<Throwable, ResultGeneration<K, R>>> x =
                                    asyncSpec.outputSpec.flatMap(oSpec -> {
                                        Optional<String> topicNameOpt = oSpec.getTopicName().apply(tuple.v1());
                                        return topicNameOpt.flatMap(tName ->
                                                oSpec.getOutputDecoder().apply(output)).map(t ->
                                                t.map(r -> new ResultGeneration<>(topicNameOpt.get(), oSpec.getSerdes(), r)));
                                    });

                            // this is just `sequence` in FP - swapping Result and Option
                            Optional<Result<Throwable, Optional<ResultGeneration<K, R>>>> y = x.map(r -> r.fold(Result::failure, r0 -> Result.success(Optional.of(r0))));
                            return y.orElseGet(() -> Result.success(Optional.empty()));
                        }));

                resultWithOutput.ifSuccessful(resultGenOpt ->
                        resultGenOpt.ifPresent(rg -> {
                            AsyncSerdes<K, R> serdes = rg.outputSerdes;
                            ProducerRecord<K, R> outputRecord = new ProducerRecord<>(rg.topicName, tuple.v2(), rg.result);
                            ProducerRecord<byte[], byte[]> byteRecord = AsyncTransform.toBytes(outputRecord, serdes.getKey(), serdes.getOutput());
                            producer.send(byteRecord);
                        }));

                publishActionResult(sagaId, request, producer, resultWithOutput);
            }
        };

        if (decodedWithKey.isFailure()) {
            publishActionResult(sagaId, request, producer, decodedWithKey);
        } else {
            Tuple2<I, K> inputWithKey = decodedWithKey.getOrElse(null);
            Callback<O> callback = cpb.apply(inputWithKey);
            asyncSpec.timeout.ifPresent(tmOut -> {
                        asyncContext.getExecutor().schedule(() -> {
                            if (completed.compareAndSet(false, true)) {
                                callback.complete(Result.failure(new TimeoutException("Timeout after " + tmOut.toString())));
                            }
                        }, tmOut.toMillis(), TimeUnit.MILLISECONDS);
                    }
            );
            asyncContext.getExecutor().execute(() -> asyncSpec.asyncFunction.accept(inputWithKey.v1(), callback));
        }
    }

    void close() {
        closed.set(true);
        consumer.ifPresent(KafkaConsumer::wakeup);
    }
}
