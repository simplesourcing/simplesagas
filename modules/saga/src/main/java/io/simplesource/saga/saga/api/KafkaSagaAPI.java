package io.simplesource.saga.saga.api;

import io.simplesource.data.FutureResult;
import io.simplesource.data.Result;
import io.simplesource.kafka.dsl.KafkaConfig;
import io.simplesource.kafka.internal.client.KafkaRequestAPI;
import io.simplesource.kafka.internal.client.RequestAPIContext;
import io.simplesource.kafka.spec.WindowSpec;
import io.simplesource.saga.model.api.SagaAPI;
import io.simplesource.saga.model.messages.SagaRequest;
import io.simplesource.saga.model.messages.SagaResponse;
import io.simplesource.saga.model.saga.SagaError;
import io.simplesource.saga.model.serdes.SagaSerdes;
import io.simplesource.saga.model.specs.SagaSpec;
import io.simplesource.saga.shared.topics.TopicConfig;
import io.simplesource.saga.shared.topics.TopicTypes;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public final class KafkaSagaAPI<A> implements SagaAPI<A> {
    private final KafkaRequestAPI<UUID, SagaRequest<A>, SagaResponse> requestApi;

    public KafkaSagaAPI(SagaSpec<A> sagaSpec,
                        KafkaConfig kConfig,
                        TopicConfig sagaTopicConfig,
                        String clientId,
                        ScheduledExecutorService scheduler) {
        SagaSerdes<A> serdes = sagaSpec.serdes;

        RequestAPIContext<UUID, SagaRequest<A>, SagaResponse> apiContext = RequestAPIContext
                .<UUID, SagaRequest<A>, SagaResponse>builder()
                .kafkaConfig(kConfig)
                .requestTopic(sagaTopicConfig.namer.apply(TopicTypes.SagaTopic.request))
                .responseTopicMapTopic(sagaTopicConfig.namer.apply(TopicTypes.SagaTopic.responseTopicMap))
                .privateResponseTopic(sagaTopicConfig.namer.apply(TopicTypes.SagaTopic.response) + "_" + clientId)
                .requestKeySerde(serdes.uuid())
                .requestValueSerde(serdes.request())
                .responseKeySerde(serdes.uuid())
                .responseValueSerde(serdes.response())
                .responseWindowSpec(new WindowSpec(TimeUnit.DAYS.toSeconds(7)))
                .outputTopicConfig(sagaTopicConfig.topicSpecs.get(TopicTypes.SagaTopic.response))
                .errorValue((request, error) -> new SagaResponse(request.sagaId, Result.failure(SagaError.of(SagaError.Reason.InternalError, error))))
                .scheduler(scheduler)
                .build();

        requestApi = new KafkaRequestAPI<>(apiContext);
    }

    @Override
    public FutureResult<SagaError, UUID> submitSaga(SagaRequest<A> request) {
        return requestApi.publishRequest(request.sagaId, request.sagaId, request)
                .errorMap(e -> SagaError.of(SagaError.Reason.InternalError, e))
                .map(r -> request.sagaId);
    }

    @Override
    public FutureResult<SagaError, SagaResponse> getSagaResponse(UUID requestId, Duration timeout) {
        return FutureResult.ofFuture(requestApi.queryResponse(requestId, timeout),
                e -> SagaError.of(SagaError.Reason.InternalError, e));
    }
}

//
//class KafkaSagaAPI<A>(sagaSpec: SagaSpec<A>,
//                      kConfig: KafkaConfig,
//                      sagaTopicConfig: TopicConfig,
//                      clientId: String,
//                      scheduler: ScheduledExecutorService)
//    extends SagaAPI<A> {
//  import FutureResultOps._
//
//  private val apiContext = RequestAPIContext
//    .builder<UUID, SagaRequest<A>, SagaResponse>
//    .kafkaConfig(kConfig)
//    .requestTopic(sagaTopicConfig.namer(TopicTypes.SagaTopic.request))
//    .responseTopicMapTopic(sagaTopicConfig.namer(TopicTypes.SagaTopic.responseTopicMap))
//    .privateResponseTopic(sagaTopicConfig.namer(TopicTypes.SagaTopic.response) + "_" + clientId)
//    .requestKeySerde(sagaSpec.serdes.uuid)
//    .requestValueSerde(sagaSpec.serdes.request)
//    .responseKeySerde(sagaSpec.serdes.uuid)
//    .responseValueSerde(sagaSpec.serdes.response)
//    .responseWindowSpec(new WindowSpec(TimeUnit.DAYS.toSeconds(7)))
//    .outputTopicConfig(sagaTopicConfig.topicSpecs(TopicTypes.SagaTopic.response))
//    .errorValue((request, error) => SagaResponse(request.sagaId, Left(SagaError.of(error.getMessage))))
//    .scheduler(scheduler)
//    .build()
//
//  private val requestAPI = new KafkaRequestAPI<UUID, SagaRequest<A>, SagaResponse>(apiContext)
//
//  override def submitSaga(request: SagaRequest<A>): Future<UUID> = {
//    val result = requestAPI.publishRequest(request.sagaId, request.sagaId, request)
//    result.asScalaFuture.map(_ => request.sagaId)
//  }
//
//  override def getSagaResponse(requestId: UUID, timeout: Duration): Future<SagaResponse> = {
//    val result: CompletableFuture<SagaResponse> =
//      requestAPI.queryResponse(requestId, java.time.Duration.ofMillis(timeout.toMillis))
//
//    result.asScalaFuture
//  }
//}

//
//import java.util.UUID
//import java.util.concurrent.{CompletableFuture, ScheduledExecutorService, TimeUnit}
//import java.util.function.BiFunction
//
//import io.simplesource.data.{FutureResult, NonEmptyList}
//import io.simplesource.kafka.dsl.KafkaConfig
//import io.simplesource.kafka.internal.client.{KafkaRequestAPI, RequestAPIContext}
//import io.simplesource.kafka.spec.WindowSpec
//import model.api.SagaAPI
//import model.messages._
//import model.saga.SagaError
//import model.specs.SagaSpec
//import shared.topics.{TopicConfig, TopicTypes}
//
//import scala.concurrent.ExecutionContext.Implicits.global
//import scala.concurrent.duration._
//import scala.concurrent.{Future, Promise}
//
//object FutureResultOps {
//  implicit class FPOps<A>(fr: FutureResult<Exception, A>) {
//    def asScalaFuture: Future<A> = {
//      val p = Promise<A>
//      fr.fold<Unit>((e: NonEmptyList<Exception>) => {
//        p.failure(e.head())
//      }, (a: A) => {
//        p.success(a)
//      })
//      p.future
//    }
//  }
//
//  implicit class JFOps<A>(jf: CompletableFuture<A>) {
//    def asScalaFuture: Future<A> = {
//      val p = Promise<A>
//      def handlerFn: BiFunction<A, Throwable, Int> = (a, e) => {
//        if (e != null) {
//          p.failure(e)
//        }
//        if (a != null) {
//          p.success(a)
//        }
//        0
//      }
//      jf.handleAsync<Int>(handlerFn)
//      p.future
//    }
//  }
//}
//
//class KafkaSagaAPI<A>(sagaSpec: SagaSpec<A>,
//                      kConfig: KafkaConfig,
//                      sagaTopicConfig: TopicConfig,
//                      clientId: String,
//                      scheduler: ScheduledExecutorService)
//    extends SagaAPI<A> {
//  import FutureResultOps._
//
//  private val apiContext = RequestAPIContext
//    .builder<UUID, SagaRequest<A>, SagaResponse>
//    .kafkaConfig(kConfig)
//    .requestTopic(sagaTopicConfig.namer(TopicTypes.SagaTopic.request))
//    .responseTopicMapTopic(sagaTopicConfig.namer(TopicTypes.SagaTopic.responseTopicMap))
//    .privateResponseTopic(sagaTopicConfig.namer(TopicTypes.SagaTopic.response) + "_" + clientId)
//    .requestKeySerde(sagaSpec.serdes.uuid)
//    .requestValueSerde(sagaSpec.serdes.request)
//    .responseKeySerde(sagaSpec.serdes.uuid)
//    .responseValueSerde(sagaSpec.serdes.response)
//    .responseWindowSpec(new WindowSpec(TimeUnit.DAYS.toSeconds(7)))
//    .outputTopicConfig(sagaTopicConfig.topicSpecs(TopicTypes.SagaTopic.response))
//    .errorValue((request, error) => SagaResponse(request.sagaId, Left(SagaError.of(error.getMessage))))
//    .scheduler(scheduler)
//    .build()
//
//  private val requestAPI = new KafkaRequestAPI<UUID, SagaRequest<A>, SagaResponse>(apiContext)
//
//  override def submitSaga(request: SagaRequest<A>): Future<UUID> = {
//    val result = requestAPI.publishRequest(request.sagaId, request.sagaId, request)
//    result.asScalaFuture.map(_ => request.sagaId)
//  }
//
//  override def getSagaResponse(requestId: UUID, timeout: Duration): Future<SagaResponse> = {
//    val result: CompletableFuture<SagaResponse> =
//      requestAPI.queryResponse(requestId, java.time.Duration.ofMillis(timeout.toMillis))
//
//    result.asScalaFuture
//  }
//}
