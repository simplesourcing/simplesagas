package io.simplesource.saga.action.async

import java.util.Properties

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.{ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.Serde

import scala.concurrent.ExecutionContext

object AsyncTransform {
  val useTransactions: Boolean = false

  trait AsyncPipe {
    def close()
  }

  implicit class ProdRecordOps[K, V](record: ProducerRecord[K, V]) {
    def toByteArray(kSerde: Serde[K], vSerde: Serde[V]): ProducerRecord[Array[Byte], Array[Byte]] = {
      val topicName = record.topic()
      val kb        = kSerde.serializer().serialize(topicName, record.key())
      val kv        = vSerde.serializer().serialize(topicName, record.value())
      new ProducerRecord[Array[Byte], Array[Byte]](topicName, kb, kv)
    }
  }

  def async[A, I, K, O, R](asyncContext: AsyncContext[A, I, K, O, R])(
      implicit executionContext: ExecutionContext): Properties => AsyncPipe = config => {
    val asyncSpec = asyncContext.asyncSpec

    def copyProperties(properties: Properties) = {
      val newProps = new Properties()
      properties.forEach((key, value) => {
        newProps.setProperty(key.toString, value.toString)
        ()
      })
      newProps
    }

    val consumerConfig: Properties = copyProperties(config)
    //consumerConfig.putAll(spec.config)
    consumerConfig.setProperty(ConsumerConfig.GROUP_ID_CONFIG,
                               asyncSpec.groupId + "_async_consumer_" + asyncSpec.actionType)
    // For now automatic - probably rather do this manually
    consumerConfig.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true")
    consumerConfig.setProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000")
    consumerConfig.setProperty(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed")

    val producerProps = copyProperties(config)
    if (useTransactions)
      producerProps.setProperty(ProducerConfig.TRANSACTIONAL_ID_CONFIG, asyncSpec.groupId + "_async_producer")
    producerProps.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")

    new AsyncPipe {
      val runner = new ConsumerRunner(asyncContext, consumerConfig, producerProps)
      new Thread(runner).start()
      override def close(): Unit = runner.close()
    }
  }
}
