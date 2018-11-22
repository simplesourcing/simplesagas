package saga.api

import java.util
import java.util.UUID
import java.util.concurrent.{CompletableFuture, ScheduledExecutorService, TimeUnit}
import java.util.function.BiFunction

import io.simplesource.data.{FutureResult, NonEmptyList}
import io.simplesource.kafka.dsl.KafkaConfig
import io.simplesource.kafka.internal.client.{KafkaRequestAPI, RequestAPIContext}
import io.simplesource.kafka.spec.WindowSpec
import model.api.SagaAPI
import model.messages._
import model.specs.SagaSpec
import shared.topics.{TopicConfig, TopicTypes}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Future, Promise}

object FutureResultOps {
  implicit class FPOps[A](fr: FutureResult[Exception, A]) {
    def asScalaFuture: Future[A] = {
      import scala.concurrent.ExecutionContext.Implicits.global
      val v: util.concurrent.Future[A] =
        fr.fold[A]((e: NonEmptyList[Exception]) => throw e.head(), (a: A) => a)
      Future { v.get() }
    }
  }

  implicit class JFOps[A](jf: CompletableFuture[A]) {
    def asScalaFuture: Future[A] = {
      val p = Promise[A]
      def handlerFn: BiFunction[A, Throwable, Int] = (a, e) => {
        if (e != null) {
          p.failure(e)
        }
        if (a != null) {
          p.success(a)
        }
        0
      }
      jf.handleAsync[Int](handlerFn)
      p.future
    }
  }
}

class KafkaSagaAPI[A](sagaSpec: SagaSpec[A],
                      kConfig: KafkaConfig,
                      sagaTopicConfig: TopicConfig,
                      clientId: String,
                      scheduler: ScheduledExecutorService)
    extends SagaAPI[A] {
  import FutureResultOps._

  private val apiContext = RequestAPIContext
    .builder[UUID, SagaRequest[A], SagaResponse]
    .kafkaConfig(kConfig)
    .requestTopic(sagaTopicConfig.namer(TopicTypes.SagaTopic.request))
    .responseTopicMapTopic(sagaTopicConfig.namer(TopicTypes.SagaTopic.responseTopicMap))
    .privateResponseTopic(sagaTopicConfig.namer(TopicTypes.SagaTopic.response) + clientId)
    .requestKeySerde(sagaSpec.serdes.uuid)
    .requestValueSerde(sagaSpec.serdes.request)
    .responseKeySerde(sagaSpec.serdes.uuid)
    .responseValueSerde(sagaSpec.serdes.response)
    .responseWindowSpec(new WindowSpec(TimeUnit.DAYS.toSeconds(7)))
    .outputTopicConfig(sagaTopicConfig.topicSpecs(TopicTypes.SagaTopic.response))
    .build()

  private val requestAPI = new KafkaRequestAPI[UUID, SagaRequest[A], SagaResponse](apiContext)

  override def submitSaga(request: SagaRequest[A], timeout: Duration): Future[SagaResponse] = {
    val result = requestAPI.publishRequest(request.sagaId, request.sagaId, request)
    result.asScalaFuture.flatMap(_ => getSagaResponse(request.sagaId, timeout))
  }

  override def getSagaResponse(requestId: UUID, timeout: Duration): Future[SagaResponse] = {
    val result: CompletableFuture[SagaResponse] =
      requestAPI.queryResponse(requestId, java.time.Duration.ofMillis(timeout.toMillis))

    result.asScalaFuture
  }
}
