package io.simplesource.saga.action.async;

import com.google.common.collect.Lists;
import io.simplesource.data.Result;
import io.simplesource.kafka.internal.util.Tuple2;
import io.simplesource.saga.model.messages.ActionRequest;
import io.simplesource.saga.model.specs.ActionProcessorSpec;
import io.simplesource.saga.shared.topics.TopicTypes;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.Serdes;
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
        consumer.subscribe(Collections.singletonList(asyncContext.actionTopicNamer.apply(TopicTypes.ActionTopic.requestUnprocessed)))

        this.consumer = Optional.of(consumer);

        try {
            while (!closed.get()) {
                ConsumerRecords<UUID, ActionRequest<A>> records = consumer.poll(Duration.ofMillis(100L));
                for (ConsumerRecord<UUID, ActionRequest<A>> x : records) {
                    UUID sagaId = x.key();
                    ActionRequest<A> request = x.value();
                    if (request.actionType.equals(asyncSpec.actionType)) {
                        processRecord(sagaId, request, producer)
                    }
                }
            }
        } catch (WakeupException e) {
            if (!closed.get()) throw e;
        } finally {
            logger.info("Closing consumer and producer")
            consumer.commitSync();
            consumer.close();
            producer.flush();
            producer.close();
        }
    }

    static <X> Result<Throwable, X> tryPure(Supplier<X> xSupplier) {
        try {
            return Result.success(xSupplier.get());
        } catch (Throwable e) {
            return Result.failure(e);
        }
    }

    static <X> Result<Throwable, X> tryWrap(Supplier<Result<Throwable, X>> xSupplier) {
        try {
            return xSupplier.get();
        } catch (Throwable e) {
            return Result.failure(e);
        }
    }

    void processRecord(UUID sagaId, ActionRequest<A> request,
                       KafkaProducer<byte[], byte[]> producer) {

        Result<Throwable, Tuple2<I, K>> decodedWithKey = tryWrap(() ->
                asyncSpec.inputDecoder.apply(request.actionCommand.command))
                .flatMap(decoded ->
                        tryPure(() ->
                                asyncSpec.keyMapper.apply(decoded)).map(k -> Tuple2.of(decoded, k)));


        Function< Tuple2<I, K>, CallBack<O>> cpb = tuple -> result -> {
            Result<Throwable, Optional<R>> resultWithOutput = result.flatMap(output -> {
                Optional<Result<Throwable, Tuple2<R, K>>> x = asyncSpec.outputSpec.flatMap((AsyncOutput<I, K, O, R> oSpec) -> {
                    Optional<String> topicNameOpt = oSpec.getTopicName().apply(tuple.v1());
                    return topicNameOpt.flatMap(tName -> oSpec.getOutputDecoder().apply(output)).map(t -> t.map());
                });
                Optional<Result<Throwable, Optional<R>>> y = x.map(r -> r.fold(Result::failure, r0 -> Result.success(Optional.of(r0))));
                return y.orElseGet(() -> Result.success(Optional.empty()));
            });
        };

        decodedWithKey.ifSuccessful(tuple -> {
            asyncSpec.asyncFunction.accept(tuple.v1(), cpb.apply(tuple));
        });

    }

//
//  def processRecord(sagaId: UUID,
//                    request: ActionRequest<A>,
//                    producer: KafkaProducer<Array<Byte>, Array<Byte>>): Unit = {
//    import AsyncTransform._
//    val tryDecode = Try {
//      val decoded = asyncSpec.inputDecoder(request.actionCommand.command)
//      decoded
//        .leftMap(error => {
//          logger.debug(s"Error decoding ${request.actionType} command:")
//          error
//        })
//        .toTry
//    }.flatten
//    val tryDecKey = tryDecode.flatMap(d => Try { asyncSpec.keyMapper(d) }.map(k => (d, k)))
//
//    val invoke =
//      tryDecKey.fold<Future<(I, K, O)>>(error => Future.failed(error), {
//        case (d, k) =>
//          asyncSpec.asyncFunction(d).map(eto => (d, k, eto))
//      })
//
//    invoke
//      .onComplete { tryDKO: Try<(I, K, O)> =>
//        try {
//          if (useTransactions)
//            producer.beginTransaction()
//
//          type RTO = (R, String, AsyncOutput<I, K, O, R>)
//          val eko: Either<Throwable, (K, Option<RTO>)> = tryDKO.toEither
//            .flatMap {
//              case (i, k, o) =>
//                asyncSpec.outputSpec
//                  .flatMap(output => output.topicName(i).map((output, _)))
//                  .fold<Either<Throwable, Option<RTO>>>(Right(None))(outputTn => {
//                    val (output, tn) = outputTn
//                    output
//                      .outputDecoder(o)
//                      .fold<Either<Throwable, Option<RTO>>>(Right(None))(_.map(r =>
//                        Option.apply((r, tn, output))))
//                  })
//                  .map((k, _))
//            }
//
//          eko.foreach {
//            case (k, Some((r, topic, output))) =>
//              val outputRecord = new ProducerRecord<K, R>(topic, k, r)
//              producer.send(outputRecord.toByteArray(output.serdes.key, output.serdes.output))
//            case _ => ()
//          }
//
//          val response =
//            eko.leftMap(error => SagaError.of(error.getMessage)).map(_ => ())
//
//          val actionResponse = ActionResponse(sagaId = request.sagaId,
//                                              actionId = request.actionId,
//                                              commandId = request.actionCommand.commandId,
//                                              response)
//          val responseRecord =
//            new ProducerRecord<UUID, ActionResponse>(
//              asyncContext.actionTopicNamer(TopicTypes.ActionTopic.response),
//              sagaId,
//              actionResponse)
//          producer.send(responseRecord.toByteArray(actionSpec.serdes.uuid, actionSpec.serdes.response))
//        } catch {
//          case error: Throwable =>
//            logger.error(error.getMessage)
//            if (useTransactions)
//              producer.abortTransaction()
//        }
//      }
}

    //
    void close() {
        closed.set(true);
        consumer.ifPresent(KafkaConsumer::wakeup);
    }
}
