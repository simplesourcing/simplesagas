package io.simplesource.saga.action.async;

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.simplesource.data.Result;
import io.simplesource.kafka.spec.TopicSpec;
import io.simplesource.saga.action.internal.AsyncActionProcessorProxy;
import io.simplesource.saga.action.internal.AsyncPublisher;
import io.simplesource.saga.avro.avro.generated.test.*;
import io.simplesource.saga.model.action.ActionCommand;
import io.simplesource.saga.model.messages.ActionRequest;
import io.simplesource.saga.model.messages.ActionResponse;
import io.simplesource.saga.model.serdes.ActionSerdes;
import io.simplesource.saga.model.specs.ActionProcessorSpec;
import io.simplesource.saga.serialization.avro.AvroSerdes;
import io.simplesource.saga.serialization.avro.SpecificSerdeUtils;
import io.simplesource.saga.shared.topics.TopicCreation;
import io.simplesource.saga.shared.topics.TopicNamer;
import io.simplesource.saga.shared.topics.TopicTypes;
import io.simplesource.saga.shared.utils.StreamAppConfig;
import io.simplesource.saga.testutils.*;
import lombok.Value;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.streams.Topology;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;

class AsyncStreamTests {

    private static String BROKER_URL = "http://localhost:9092";
    private static String SCHEMA_URL = "http://localhost:8081/";

    private static String TOPIC_BASE_NAME = "topic-base-name";
    private static String ASYNC_TEST_OUTPUT_TOPIC = "async_test_topic";

    private static ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);

    @AfterAll
    static void shutdown() {
        executor.shutdown();
    }

    @Value
    private static class AsyncTestContext {
        final TestContext testContext;

        // serdes
        final ActionSerdes<SpecificRecord> actionSerdes = AvroSerdes.Specific.actionSerdes(SCHEMA_URL, true);

        // publishers
        final RecordPublisher<UUID, ActionRequest<SpecificRecord>> actionRequestPublisher;
        final RecordPublisher<UUID, ActionResponse> actionResponsePublisher;
        final RecordPublisher<AsyncTestId, AsyncTestOutput> actionOutputPublisher;

        // verifiers
        final RecordVerifier<UUID, ActionRequest<SpecificRecord>> actionUnprocessedRequestVerifier;

        final MockSchemaRegistryClient regClient = new MockSchemaRegistryClient();

        private final AsyncSerdes<AsyncTestId, AsyncTestOutput> asyncSerdes;
        private final AsyncContext<SpecificRecord, AsyncTestCommand, AsyncTestId, Integer, AsyncTestOutput> asyncContext;

        private AsyncTestContext(int executionDelayMillis, Optional<Duration> timeout, BiConsumer<AsyncTestCommand, Callback<Integer>> asyncFunctionOverride) {
            asyncSerdes = new AsyncSerdes<>(SpecificSerdeUtils.specificAvroSerde(SCHEMA_URL, true, regClient),
                    SpecificSerdeUtils.specificAvroSerde(SCHEMA_URL, false, regClient));

            BiConsumer<AsyncTestCommand, Callback<Integer>> asyncFunction = (asyncFunctionOverride != null) ?
                    asyncFunctionOverride :
                    (i, callBack) -> executor.schedule(() ->
                            callBack.complete(Result.success(i.getValue() * i.getValue())), executionDelayMillis, TimeUnit.MILLISECONDS);

            AsyncSpec<SpecificRecord, AsyncTestCommand, AsyncTestId, Integer, AsyncTestOutput> asyncSpec = new AsyncSpec<>(Constants.asyncTestActionType,
                    a -> Result.success((AsyncTestCommand) a),
                    AsyncTestCommand::getId,
                    asyncFunction,
                    "group_id",
                    Optional.of(new AsyncOutput<>(
                            o -> Optional.of(Result.success(new AsyncTestOutput(o))),
                            asyncSerdes,
                            x -> Optional.of(ASYNC_TEST_OUTPUT_TOPIC),
                            Collections.singletonList(new TopicCreation(ASYNC_TEST_OUTPUT_TOPIC, new TopicSpec(6, (short) 1, Collections.emptyMap()))
                            ))),
                    timeout);

            AsyncApp<SpecificRecord> asyncApp = new AsyncApp<>(actionSerdes,
                    TopicUtils.buildSteps(Constants.actionTopicPrefix, TOPIC_BASE_NAME));
            asyncApp.addAsync(asyncSpec);
            Topology topology = asyncApp.buildTopology(new StreamAppConfig("app-id", BROKER_URL));
            topology.addSource("TO", ASYNC_TEST_OUTPUT_TOPIC);
            testContext = TestContextBuilder.of(topology).build();

            // get actionRequestPublisher
            actionRequestPublisher = testContext.publisher(
                    TopicNamer.forPrefix(Constants.actionTopicPrefix, TOPIC_BASE_NAME)
                            .apply(TopicTypes.ActionTopic.request),
                    actionSerdes.uuid(),
                    actionSerdes.request());

            actionResponsePublisher = testContext.publisher(
                    TopicNamer.forPrefix(Constants.actionTopicPrefix, TOPIC_BASE_NAME)
                            .apply(TopicTypes.ActionTopic.response),
                    actionSerdes.uuid(),
                    actionSerdes.response());

            actionOutputPublisher = testContext.publisher(
                    ASYNC_TEST_OUTPUT_TOPIC,
                    asyncSerdes.key,
                    asyncSerdes.output);

            actionUnprocessedRequestVerifier = testContext.verifier(
                    TopicNamer.forPrefix(Constants.actionTopicPrefix, TOPIC_BASE_NAME)
                            .apply(TopicTypes.ActionTopic.requestUnprocessed),
                    actionSerdes.uuid(),
                    actionSerdes.request());

            asyncContext = new AsyncContext<>(
                    new ActionProcessorSpec<>(actionSerdes),
                    TopicNamer.forPrefix(Constants.actionTopicPrefix, TOPIC_BASE_NAME),
                    asyncSpec,
                    executor);
        }

        static AsyncTestContext of(int executionDelayMillis) {
            return new AsyncTestContext(executionDelayMillis, Optional.empty(), null);
        }

        static AsyncTestContext of(int executionDelayMillis, int timeoutMillis) {
            return new AsyncTestContext(executionDelayMillis, Optional.of(Duration.ofMillis(timeoutMillis)), null);
        }

        static AsyncTestContext of(BiConsumer<AsyncTestCommand, Callback<Integer>> asyncFunction) {
            return new AsyncTestContext(0, Optional.empty(), asyncFunction);
        }
    }

    private static ActionRequest<SpecificRecord> createRequest(AsyncTestCommand AsyncTestCommand, UUID commandId) {
        ActionCommand<SpecificRecord> actionCommand = new ActionCommand<>(commandId, AsyncTestCommand);
        return ActionRequest.<SpecificRecord>builder()
                .sagaId(UUID.randomUUID())
                .actionId(UUID.randomUUID())
                .actionCommand(actionCommand)
                .actionType(Constants.asyncTestActionType)
                .build();
    }


    @Value
    private static class ValidationRecord<K, V> {
        final K key;
        final V value;
    }

    @Value
    private static class AsyncValidation {
        final List<ValidationRecord<UUID, ActionResponse>> responseRecords = new ArrayList<>();
        final List<ValidationRecord<AsyncTestId, AsyncTestOutput>> outputRecords = new ArrayList<>();
        final String responseTopic = TopicNamer.forPrefix(Constants.actionTopicPrefix, TOPIC_BASE_NAME)
                .apply(TopicTypes.ActionTopic.response);

        private final RecordPublisher<UUID, ActionResponse> actionResponsePublisher;
        final AsyncPublisher<UUID, ActionResponse> responseProducer;

        final Function<AsyncSerdes<AsyncTestId, AsyncTestOutput>, AsyncPublisher<AsyncTestId, AsyncTestOutput>> outputProducer;

        AsyncValidation(RecordPublisher<UUID, ActionResponse> actionResponsePublisher) {
            this.actionResponsePublisher = actionResponsePublisher;
            this.responseProducer = (topic, key, value) -> {
                assertThat(topic).isEqualTo(responseTopic);
                responseRecords.add(new ValidationRecord<>(key, value));
                if (actionResponsePublisher != null)
                    actionResponsePublisher.publish(key, value);
            };
            this.outputProducer = serdes -> (topic, key, value) -> {
                assertThat(topic).isEqualTo(ASYNC_TEST_OUTPUT_TOPIC);
                outputRecords.add(new ValidationRecord<>(key, value));
            };
        }

        static AsyncValidation create() { return new AsyncValidation(null);}
        static AsyncValidation create(RecordPublisher<UUID, ActionResponse> actionResponsePublisher) { return new AsyncValidation(actionResponsePublisher);}
    }

    private static void delayMillis(int millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
        }
    }

    @Test
    void actionRequestGetUnprocessed() {

        AsyncTestContext acc = AsyncTestContext.of(100);

        AsyncTestCommand accountCommand = new AsyncTestCommand(new AsyncTestId("id"), 12);

        ActionRequest<SpecificRecord> actionRequest = createRequest(accountCommand, UUID.randomUUID());

        acc.actionRequestPublisher().publish(actionRequest.sagaId, actionRequest);

        acc.actionUnprocessedRequestVerifier.verifySingle((id, req) -> {
            assertThat(id).isEqualTo(actionRequest.sagaId());
            assertThat(req).isEqualToComparingFieldByField(actionRequest);
        });
    }

    @Test
    void publishesResponseAndOutput() {
        AsyncTestContext acc = AsyncTestContext.of(100);
        AsyncValidation validation = AsyncValidation.create();

        AsyncTestCommand accountCommand = new AsyncTestCommand(new AsyncTestId("id"), 12);
        ActionRequest<SpecificRecord> actionRequest = createRequest(new AsyncTestCommand(new AsyncTestId("id"), 12), UUID.randomUUID());
        acc.actionRequestPublisher().publish(actionRequest.sagaId, actionRequest);

        AsyncActionProcessorProxy.processRecord(acc.asyncContext, actionRequest.sagaId, actionRequest, validation.responseProducer, validation.outputProducer);

        delayMillis(200);
        assertThat(validation.outputRecords).hasSize(1);
        assertThat(validation.outputRecords.get(0).key).isEqualToComparingFieldByField(accountCommand.getId());
        assertThat(validation.outputRecords.get(0).value.getValue()).isEqualTo(144);

        assertThat(validation.responseRecords).hasSize(1);
        assertThat(validation.responseRecords.get(0).key).isEqualTo(actionRequest.sagaId);
        assertThat(validation.responseRecords.get(0).value.result.isSuccess()).isTrue();
    }

    @Test
    void executesWithDelay() {
        AsyncTestContext acc = AsyncTestContext.of(99);

        AsyncTestId testId = new AsyncTestId("id");
        AsyncTestCommand accountCommand = new AsyncTestCommand(testId, 12);

        ActionRequest<SpecificRecord> actionRequest = createRequest(accountCommand, UUID.randomUUID());
        acc.actionRequestPublisher().publish(actionRequest.sagaId, actionRequest);

        AsyncValidation validation = AsyncValidation.create();
        AsyncActionProcessorProxy.processRecord(acc.asyncContext, actionRequest.sagaId, actionRequest, validation.responseProducer, validation.outputProducer);

        assertThat(validation.outputRecords).hasSize(0);
        assertThat(validation.responseRecords).hasSize(0);
    }

    @Test
    void timeoutLargeEnough() {
        AsyncTestContext acc = AsyncTestContext.of(200, 300);

        AsyncTestId testId = new AsyncTestId("id");
        AsyncTestCommand accountCommand = new AsyncTestCommand(testId, 12);

        ActionRequest<SpecificRecord> actionRequest = createRequest(accountCommand, UUID.randomUUID());
        acc.actionRequestPublisher().publish(actionRequest.sagaId, actionRequest);

        AsyncValidation validation = AsyncValidation.create();
        AsyncActionProcessorProxy.processRecord(acc.asyncContext, actionRequest.sagaId, actionRequest, validation.responseProducer, validation.outputProducer);

        delayMillis(300);

        assertThat(validation.outputRecords).hasSize(1);
        assertThat(validation.responseRecords).hasSize(1);
        assertThat(validation.responseRecords.get(0).value.result.isSuccess()).isTrue();
    }

    @Test
    void timeoutTooShort() {
        AsyncTestContext acc = AsyncTestContext.of(201, 50);

        AsyncTestId testId = new AsyncTestId("id");
        AsyncTestCommand accountCommand = new AsyncTestCommand(testId, 12);

        ActionRequest<SpecificRecord> actionRequest = createRequest(accountCommand, UUID.randomUUID());
        acc.actionRequestPublisher().publish(actionRequest.sagaId, actionRequest);

        AsyncValidation validation = AsyncValidation.create();
        AsyncActionProcessorProxy.processRecord(acc.asyncContext, actionRequest.sagaId, actionRequest, validation.responseProducer, validation.outputProducer);

        delayMillis(300);

        assertThat(validation.outputRecords).hasSize(0);
        assertThat(validation.responseRecords).hasSize(1);
        assertThat(validation.responseRecords.get(0).value.result.isFailure()).isTrue();
    }

    @Test
    void returnsAnError() {
        AsyncTestContext acc = AsyncTestContext.of((i, callBack) -> executor.schedule(() ->
                callBack.complete(Result.failure(new Exception("Exception occurred"))), 100, TimeUnit.MILLISECONDS));

        AsyncTestId testId = new AsyncTestId("id");
        AsyncTestCommand accountCommand = new AsyncTestCommand(testId, 12);

        ActionRequest<SpecificRecord> actionRequest = createRequest(accountCommand, UUID.randomUUID());
        acc.actionRequestPublisher().publish(actionRequest.sagaId, actionRequest);

        AsyncValidation validation = AsyncValidation.create();
        AsyncActionProcessorProxy.processRecord(acc.asyncContext, actionRequest.sagaId, actionRequest, validation.responseProducer, validation.outputProducer);

        delayMillis(300);

        assertThat(validation.outputRecords).hasSize(0);
        assertThat(validation.responseRecords).hasSize(1);
        assertThat(validation.responseRecords.get(0).value.result.isFailure()).isTrue();
    }

    @Test
    void handlesAnException() {
        AsyncTestContext acc = AsyncTestContext.of((i, callBack) -> {
            throw new RuntimeException("An exception was thrown");
        });

        // NOTE: there doesn't seem to be a way to catch an exception of the following form:
        // AsyncTestContext.of((i, callBack) -> executor.schedule(() ->
        // { throw new Exception("An exception was thrown"); }, 100, TimeUnit.MILLISECONDS));
        // Not sure if there should though
        AsyncTestId testId = new AsyncTestId("id");
        AsyncTestCommand accountCommand = new AsyncTestCommand(testId, 12);

        ActionRequest<SpecificRecord> actionRequest = createRequest(accountCommand, UUID.randomUUID());
        acc.actionRequestPublisher().publish(actionRequest.sagaId, actionRequest);

        AsyncValidation validation = AsyncValidation.create();
        AsyncActionProcessorProxy.processRecord(acc.asyncContext, actionRequest.sagaId, actionRequest, validation.responseProducer, validation.outputProducer);

        delayMillis(100);

        assertThat(validation.outputRecords).hasSize(0);
        assertThat(validation.responseRecords).hasSize(1);
        assertThat(validation.responseRecords.get(0).value.result.isFailure()).isTrue();
    }


    @Test
    void actionIndempotence() {

        AsyncTestContext acc = AsyncTestContext.of(100);
        AsyncValidation validation = AsyncValidation.create(acc.actionResponsePublisher);

        ActionRequest<SpecificRecord> actionRequest = createRequest(new AsyncTestCommand(new AsyncTestId("id"), 12), UUID.randomUUID());
        acc.actionRequestPublisher().publish(actionRequest.sagaId, actionRequest);

        acc.actionUnprocessedRequestVerifier.verifySingle((id, req) -> { });

        AsyncActionProcessorProxy.processRecord(acc.asyncContext, actionRequest.sagaId, actionRequest, validation.responseProducer, validation.outputProducer);

        delayMillis(200);
        assertThat(validation.responseRecords).hasSize(1);

        acc.actionRequestPublisher().publish(actionRequest.sagaId, actionRequest);

        // does not generate an additional request
        acc.actionUnprocessedRequestVerifier().verifyNoRecords();
    }

}