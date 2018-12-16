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
