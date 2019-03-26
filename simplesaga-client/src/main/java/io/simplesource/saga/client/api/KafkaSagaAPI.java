package io.simplesource.saga.client.api;

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
import io.simplesource.saga.model.saga.SagaId;
import io.simplesource.saga.model.serdes.SagaSerdes;
import io.simplesource.saga.model.specs.SagaSpec;
import io.simplesource.saga.shared.topics.TopicConfig;
import io.simplesource.saga.shared.topics.TopicTypes;
import io.simplesource.saga.shared.streams.StreamAppUtils;

import java.time.Duration;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public final class KafkaSagaAPI<A> implements SagaAPI<A> {
    private final KafkaRequestAPI<SagaId, SagaRequest<A>, SagaId, SagaResponse> requestApi;

    public KafkaSagaAPI(SagaSpec<A> sagaSpec,
                        KafkaConfig kConfig,
                        TopicConfig sagaTopicConfig,
                        String clientId,
                        ScheduledExecutorService scheduler) {
        SagaSerdes<A> serdes = sagaSpec.serdes;

        RequestAPIContext<SagaId, SagaRequest<A>, SagaId, SagaResponse> apiContext = RequestAPIContext
                .<SagaId, SagaRequest<A>, SagaId, SagaResponse>builder()
                .kafkaConfig(kConfig)
                .requestTopic(sagaTopicConfig.namer.apply(TopicTypes.SagaTopic.request))
                .responseTopicMapTopic(sagaTopicConfig.namer.apply(TopicTypes.SagaTopic.responseTopicMap))
                .privateResponseTopic(sagaTopicConfig.namer.apply(TopicTypes.SagaTopic.response) + "_" + clientId)
                .requestKeySerde(serdes.sagaId())
                .requestValueSerde(serdes.request())
                .responseKeySerde(serdes.sagaId())
                .responseValueSerde(serdes.response())
                .responseWindowSpec(new WindowSpec(TimeUnit.DAYS.toSeconds(7)))
                .outputTopicConfig(sagaTopicConfig.topicSpecs.get(TopicTypes.SagaTopic.response))
                .errorValue((request, error) -> new SagaResponse(request.sagaId, Result.failure(SagaError.of(SagaError.Reason.InternalError, error))))
                .scheduler(scheduler)
                .build();

        requestApi = new KafkaRequestAPI<>(apiContext);

        StreamAppUtils.addShutdownHook(() ->  {
            StreamAppUtils.shutdownExecutorService(scheduler);
            requestApi.close();
        });
    }

    @Override
    public FutureResult<SagaError, SagaId> submitSaga(SagaRequest<A> request) {
        return requestApi.publishRequest(request.sagaId, request.sagaId, request)
                .errorMap(e -> SagaError.of(SagaError.Reason.InternalError, e))
                .map(r -> request.sagaId);
    }

    @Override
    public FutureResult<SagaError, SagaResponse> getSagaResponse(SagaId requestId, Duration timeout) {
        return FutureResult.ofFuture(requestApi.queryResponse(requestId, timeout),
                e -> SagaError.of(SagaError.Reason.InternalError, e));
    }
}
