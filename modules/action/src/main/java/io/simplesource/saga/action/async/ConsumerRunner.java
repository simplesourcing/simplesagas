package io.simplesource.saga.action.async;

import io.simplesource.saga.model.messages.ActionRequest;
import io.simplesource.saga.model.specs.ActionProcessorSpec;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.Serdes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.simplesource.saga.action.async.AsyncTransform.*;


class ConsumerRunner<A, I, K, O, R> implements Runnable {

    private final AsyncSpec<A, I, K, O, R> asyncSpec;
    private final ActionProcessorSpec<A> actionSpec;
    AtomicBoolean closed;
    Optional<KafkaConsumer<UUID, ActionRequest<A>>> consumer = Optional.empty();
    Logger logger = LoggerFactory.getLogger(ConsumerRunner.class);
    Properties consumerConfig;
    Properties producerProps;

    ConsumerRunner(
            AsyncContext<A, I, K, O, R> asyncContext,
            Properties consumerConfig,
            Properties producerProps) {
        asyncSpec = asyncContext.asyncSpec;
        actionSpec = asyncContext.actionSpec;
        closed = new AtomicBoolean(false);
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
    
    
//

        KafkaConsumer<UUID, ActionRequest<A>> cons = new KafkaConsumer<UUID, ActionRequest<A>>(consumerConfig,
                actionSpec.serdes.uuid().deserializer(),
                actionSpec.serdes.request().deserializer());
//    consumer.subscribe(List(asyncContext.actionTopicNamer(TopicTypes.ActionTopic.requestUnprocessed)).asJava)
//    this.consumer = Some(consumer)
//
//    try {
//      while (!closed.get()) {
//        val records: ConsumerRecords<UUID, ActionRequest<A>> = consumer.poll(Duration.ofMillis(100L))
//
//        records.iterator().forEachRemaining { x =>
//          val sagaId                    = x.key()
//          val request: ActionRequest<A> = x.value()
//          if (request.actionType == asyncSpec.actionType) {
//            processRecord(sagaId, request, producer)
//          }
//        }
//      }
//    } catch {
//      case e: WakeupException => if (!closed.get) throw e
//    } finally {
//      logger.info("Closing consumer and producer")
//      consumer.commitSync()
//      consumer.close()
//      producer.flush()
//      producer.close()
//    }
//  }
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
//  def close(): Unit = {
//    closed.set(true)
//    consumer.foreach(_.wakeup())
//  }
}
