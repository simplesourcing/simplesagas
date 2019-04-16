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
import io.simplesource.saga.model.serdes.SagaClientSerdes;
import io.simplesource.saga.model.specs.SagaClientSpec;
import io.simplesource.saga.shared.topics.TopicConfig;
import io.simplesource.saga.shared.topics.TopicTypes;
import io.simplesource.saga.shared.app.StreamAppUtils;

import java.time.Duration;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * The KafkaSagaAPI is an implementation of the SagaAPI.
 * <p>
 * This provides a mechanism to invoke a saga from any Java code, and receive the saga response asynchronously.
 * <p>
 * The client can be hosted in a separate process. It only needs to be able to access the same cluster as the {@link io.simplesource.saga.saga.app.SagaApp SagaApp} is running on, and share the same serdes for the saga request and response topics.
 *
 * @param <A> a representation of an action command that is shared across all actions in the saga. This is typically a generic type, such as Json, or if using Avro serialization, SpecificRecord or GenericRecord
 */
public final class KafkaSagaAPI<A> implements SagaAPI<A> {
    private final KafkaRequestAPI<SagaId, SagaRequest<A>, SagaId, SagaResponse> requestApi;

    private KafkaSagaAPI(SagaClientSpec<A> sagaSpec,
                        KafkaConfig kConfig,
                        TopicConfig sagaTopicConfig,
                        String clientId,
                        ScheduledExecutorService scheduler) {
        SagaClientSerdes<A> serdes = sagaSpec.serdes;

        RequestAPIContext<SagaId, SagaRequest<A>, SagaId, SagaResponse> apiContext = RequestAPIContext
                .<SagaId, SagaRequest<A>, SagaId, SagaResponse>builder()
                .kafkaConfig(kConfig)
                .requestTopic(sagaTopicConfig.namer.apply(TopicTypes.SagaTopic.SAGA_REQUEST))
                .responseTopicMapTopic(sagaTopicConfig.namer.apply(TopicTypes.SagaTopic.SAGA_RESPONSE_TOPIC_MAP))
                .privateResponseTopic(sagaTopicConfig.namer.apply(TopicTypes.SagaTopic.SAGA_RESPONSE) + "_" + clientId)
                .requestKeySerde(serdes.sagaId())
                .requestValueSerde(serdes.request())
                .responseKeySerde(serdes.sagaId())
                .responseValueSerde(serdes.response())
                .responseWindowSpec(new WindowSpec(TimeUnit.DAYS.toSeconds(7)))
                .outputTopicConfig(sagaTopicConfig.topicSpecs.get(TopicTypes.SagaTopic.SAGA_RESPONSE))
                .errorValue((request, error) -> SagaResponse.of(request.sagaId, Result.failure(SagaError.of(SagaError.Reason.InternalError, error))))
                .scheduler(scheduler)
                .uuidToResponseId(SagaId::of)
                .responseIdToUuid(SagaId::id)
                .build();

        requestApi = new KafkaRequestAPI<>(apiContext);

        StreamAppUtils.addShutdownHook(() ->  {
            StreamAppUtils.shutdownExecutorService(scheduler);
            requestApi.close();
        });
    }

    /**
     * Creates an instance of the Saga API.
     *
     * @param sagaSpec        A data structure with saga configuration details
     * @param kConfig         the kafka configuration
     * @param sagaTopicConfig the saga topic configuration
     * @param clientId        this is used to identify the client. If should be unique for a given client. Saga responses are funneled into separate topics per client ID. This saves multiple clients from having to consume the responses for all sagas - they only consume the responses from their private topic.
     * @param scheduler       the scheduler for scheduling timeouts
     */
    static <A> SagaAPI<A> of(SagaClientSpec<A> sagaSpec,
                             KafkaConfig kConfig,
                             TopicConfig sagaTopicConfig,
                             String clientId,
                             ScheduledExecutorService scheduler) {
        return new KafkaSagaAPI<>(sagaSpec, kConfig, sagaTopicConfig, clientId, scheduler);
    }


    @Override
    public FutureResult<SagaError, SagaId> submitSaga(SagaRequest<A> request) {
        return requestApi.publishRequest(request.sagaId, request.sagaId, request)
                .errorMap(e -> SagaError.of(SagaError.Reason.InternalError, e))
                .map(r -> request.sagaId);
    }

    @Override
    public FutureResult<SagaError, SagaResponse> getSagaResponse(SagaId sagaId, Duration timeout) {
        return FutureResult.ofFuture(requestApi.queryResponse(sagaId, timeout),
                e -> SagaError.of(SagaError.Reason.InternalError, e));
    }
}
