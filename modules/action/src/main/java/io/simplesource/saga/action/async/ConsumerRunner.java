package io.simplesource.saga.action.async

import java.time.Duration
import java.util.concurrent.atomic.AtomicBoolean
import java.util.{Properties, UUID}

import cats.implicits._
import model.messages.{ActionRequest, ActionResponse}
import model.saga.SagaError
import org.apache.kafka.clients.consumer.{ConsumerRecords, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.errors.WakeupException
import org.apache.kafka.common.serialization.Serdes
import org.slf4j.LoggerFactory
import shared.topics.TopicTypes

import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

class ConsumerRunner[A, I, K, O, R](asyncContext: AsyncContext[A, I, K, O, R],
                                    consumerConfig: Properties,
                                    producerProps: Properties)(implicit executionContext: ExecutionContext)
    extends Runnable {
  private val asyncSpec                                       = asyncContext.asyncSpec
  private val actionSpec                                      = asyncContext.actionSpec
  private val closed                                          = new AtomicBoolean(false)
  var consumer: Option[KafkaConsumer[UUID, ActionRequest[A]]] = None

  private val logger = LoggerFactory.getLogger(classOf[ConsumerRunner[A, I, K, O, R]])

  override def run(): Unit = {
    import AsyncTransform._
    val producer: KafkaProducer[Array[Byte], Array[Byte]] =
      new KafkaProducer[Array[Byte], Array[Byte]](producerProps,
                                                  Serdes.ByteArray().serializer(),
                                                  Serdes.ByteArray().serializer())
    if (useTransactions)
      producer.initTransactions()

    val consumer = new KafkaConsumer[UUID, ActionRequest[A]](consumerConfig,
                                                             actionSpec.serdes.uuid.deserializer(),
                                                             actionSpec.serdes.request.deserializer())
    consumer.subscribe(List(asyncContext.actionTopicNamer(TopicTypes.ActionTopic.requestUnprocessed)).asJava)
    this.consumer = Some(consumer)

    try {
      while (!closed.get()) {
        val records: ConsumerRecords[UUID, ActionRequest[A]] = consumer.poll(Duration.ofMillis(100L))

        records.iterator().forEachRemaining { x =>
          val sagaId                    = x.key()
          val request: ActionRequest[A] = x.value()
          if (request.actionType == asyncSpec.actionType) {
            processRecord(sagaId, request, producer)
          }
        }
      }
    } catch {
      case e: WakeupException => if (!closed.get) throw e
    } finally {
      logger.info("Closing consumer and producer")
      consumer.commitSync()
      consumer.close()
      producer.flush()
      producer.close()
    }
  }

  def processRecord(sagaId: UUID,
                    request: ActionRequest[A],
                    producer: KafkaProducer[Array[Byte], Array[Byte]]): Unit = {
    import AsyncTransform._
    val tryDecode = Try {
      val decoded = asyncSpec.inputDecoder(request.actionCommand.command)
      decoded
        .leftMap(error => {
          logger.debug(s"Error decoding ${request.actionType} command:")
          error
        })
        .toTry
    }.flatten
    val tryDecKey = tryDecode.flatMap(d => Try { asyncSpec.keyMapper(d) }.map(k => (d, k)))

    val invoke =
      tryDecKey.fold[Future[(I, K, O)]](error => Future.failed(error), {
        case (d, k) =>
          asyncSpec.asyncFunction(d).map(eto => (d, k, eto))
      })

    invoke
      .onComplete { tryDKO: Try[(I, K, O)] =>
        try {
          if (useTransactions)
            producer.beginTransaction()

          type RTO = (R, String, AsyncOutput[I, K, O, R])
          val eko: Either[Throwable, (K, Option[RTO])] = tryDKO.toEither
            .flatMap {
              case (i, k, o) =>
                asyncSpec.outputSpec
                  .flatMap(output => output.topicName(i).map((output, _)))
                  .fold[Either[Throwable, Option[RTO]]](Right(None))(outputTn => {
                    val (output, tn) = outputTn
                    output
                      .outputDecoder(o)
                      .fold[Either[Throwable, Option[RTO]]](Right(None))(_.map(r =>
                        Option.apply((r, tn, output))))
                  })
                  .map((k, _))
            }

          eko.foreach {
            case (k, Some((r, topic, output))) =>
              val outputRecord = new ProducerRecord[K, R](topic, k, r)
              producer.send(outputRecord.toByteArray(output.serdes.key, output.serdes.output))
            case _ => ()
          }

          val response =
            eko.leftMap(error => SagaError.of(error.getMessage)).map(_ => ())

          val actionResponse = ActionResponse(sagaId = request.sagaId,
                                              actionId = request.actionId,
                                              commandId = request.actionCommand.commandId,
                                              response)
          val responseRecord =
            new ProducerRecord[UUID, ActionResponse](
              asyncContext.actionTopicNamer(TopicTypes.ActionTopic.response),
              sagaId,
              actionResponse)
          producer.send(responseRecord.toByteArray(actionSpec.serdes.uuid, actionSpec.serdes.response))
        } catch {
          case error: Throwable =>
            logger.error(error.getMessage)
            if (useTransactions)
              producer.abortTransaction()
        }
      }
  }

  def close(): Unit = {
    closed.set(true)
    consumer.foreach(_.wakeup())
  }
}
