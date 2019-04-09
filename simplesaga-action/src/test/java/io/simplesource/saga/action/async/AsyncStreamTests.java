package io.simplesource.saga.action.async;

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.simplesource.api.CommandId;
import io.simplesource.data.Result;
import io.simplesource.saga.action.ActionApp;
import io.simplesource.saga.action.app.ActionProcessor;
import io.simplesource.saga.model.messages.UndoCommand;
import io.simplesource.saga.model.serdes.TopicSerdes;
import io.simplesource.saga.action.internal.AsyncActionProcessorProxy;
import io.simplesource.saga.shared.kafka.AsyncPublisher;
import io.simplesource.saga.avro.avro.generated.test.AsyncTestCommand;
import io.simplesource.saga.avro.avro.generated.test.AsyncTestId;
import io.simplesource.saga.avro.avro.generated.test.AsyncTestOutput;
import io.simplesource.saga.model.action.ActionCommand;
import io.simplesource.saga.model.action.ActionId;
import io.simplesource.saga.model.messages.ActionRequest;
import io.simplesource.saga.model.messages.ActionResponse;
import io.simplesource.saga.model.saga.SagaId;
import io.simplesource.saga.model.serdes.ActionSerdes;
import io.simplesource.saga.model.specs.ActionSpec;
import io.simplesource.saga.serialization.avro.AvroSerdes;
import io.simplesource.saga.serialization.avro.SpecificSerdeUtils;
import io.simplesource.saga.shared.streams.StreamBuildResult;
import io.simplesource.saga.shared.topics.TopicNamer;
import io.simplesource.saga.shared.topics.TopicTypes;
import io.simplesource.saga.shared.streams.StreamAppConfig;
import io.simplesource.saga.shared.topics.TopicUtils;
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
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

class AsyncStreamTests {

    private static String SCHEMA_URL = "http://localhost:8081/";

    private static String ASYNC_TEST_OUTPUT_TOPIC = "async_test_topic";

    private static ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);

    @AfterAll
    static void shutdown() {
        executor.shutdown();
    }

    @Value
    private static class AsyncTestContext {
        final TestContext testContext;
        final Set<String> expectedTopics;

        // serdes
        final ActionSerdes<SpecificRecord> actionSerdes = AvroSerdes.Specific.actionSerdes(SCHEMA_URL, true);

        // publishers
        final RecordPublisher<SagaId, ActionRequest<SpecificRecord>> actionRequestPublisher;
        final RecordPublisher<SagaId, ActionResponse<SpecificRecord>> actionResponsePublisher;
        final RecordPublisher<AsyncTestId, AsyncTestOutput> actionOutputPublisher;

        // verifiers
        final RecordVerifier<SagaId, ActionRequest<SpecificRecord>> actionUnprocessedRequestVerifier;

        final MockSchemaRegistryClient regClient = new MockSchemaRegistryClient();

        private final TopicSerdes<AsyncTestId, AsyncTestOutput> asyncSerdes;
        private final AsyncContext<SpecificRecord, AsyncTestCommand, AsyncTestId, Double, AsyncTestOutput> asyncContext;

        private AsyncTestContext(int executionDelayMillis, Optional<Duration> timeout, BiConsumer<AsyncTestCommand, Callback<Double>> asyncFunctionOverride) {
            asyncSerdes = new TopicSerdes<>(SpecificSerdeUtils.specificAvroSerde(SCHEMA_URL, true, regClient),
                    SpecificSerdeUtils.specificAvroSerde(SCHEMA_URL, false, regClient));

            ActionApp<SpecificRecord> actionApp = ActionApp.of(actionSerdes);

            AsyncSpec<SpecificRecord, AsyncTestCommand, AsyncTestId, Double, AsyncTestOutput> asyncSpec =
                    getAsyncSpec(executionDelayMillis, timeout, asyncFunctionOverride);
            AsyncSpec<SpecificRecord, AsyncTestCommand, AsyncTestId, Double, AsyncTestOutput> undoSpec =
                    getAsyncSpec(executionDelayMillis, timeout, (i, callBack) -> executor.schedule(() ->
                            callBack.complete(Result.success(Math.sqrt(i.getValue()))), executionDelayMillis, TimeUnit.MILLISECONDS));

            actionApp.withActionProcessor(getActionProcessor(asyncSpec));

            StreamBuildResult sb = actionApp.build(pb -> pb.withStreamAppConfig(StreamAppConfig.of("app-id", "http://localhost:9092")));
            Topology topology = sb.topologySupplier.get();
            expectedTopics = sb.topicCreations.stream().map(x -> x.topicName).collect(Collectors.toSet());

            testContext = TestContextBuilder.of(topology).build();

            TopicNamer topicNamer = TopicUtils.topicNamerOverride(
                    TopicNamer.forPrefix(Constants.ACTION_TOPIC_PREFIX, TopicUtils.actionTopicBaseName(Constants.ASYNC_TEST_ACTION_TYPE)),
                    Collections.singletonMap(TopicTypes.ActionTopic.ACTION_OUTPUT, ASYNC_TEST_OUTPUT_TOPIC));

            // get actionRequestPublisher
            actionRequestPublisher = testContext.publisher(
                    topicNamer.apply(TopicTypes.ActionTopic.ACTION_REQUEST),
                    actionSerdes.sagaId(),
                    actionSerdes.request());

            actionResponsePublisher = testContext.publisher(
                    topicNamer.apply(TopicTypes.ActionTopic.ACTION_RESPONSE),
                    actionSerdes.sagaId(),
                    actionSerdes.response());

            actionOutputPublisher = testContext.publisher(
                    ASYNC_TEST_OUTPUT_TOPIC,
                    asyncSerdes.key,
                    asyncSerdes.value);

            actionUnprocessedRequestVerifier = testContext.verifier(
                    topicNamer.apply(TopicTypes.ActionTopic.ACTION_REQUEST_UNPROCESSED),
                    actionSerdes.sagaId(),
                    actionSerdes.request());

            asyncContext = new AsyncContext<>(
                    ActionSpec.of(actionSerdes),
                    topicNamer,
                    asyncSpec,
                    executor);
        }

        private ActionProcessor<SpecificRecord> getActionProcessor(AsyncSpec<SpecificRecord, AsyncTestCommand, AsyncTestId, Double, AsyncTestOutput> asyncSpec) {
            return AsyncBuilder.apply(
                            asyncSpec,
                            topicBuilder -> topicBuilder
                                    .withTopicPrefix(Constants.ACTION_TOPIC_PREFIX)
                                    .withTopicNameOverride(TopicTypes.ActionTopic.ACTION_OUTPUT, ASYNC_TEST_OUTPUT_TOPIC)
                    );
        }

        private AsyncSpec<SpecificRecord, AsyncTestCommand, AsyncTestId, Double, AsyncTestOutput> getAsyncSpec(int executionDelayMillis, Optional<Duration> timeout, BiConsumer<AsyncTestCommand, Callback<Double>> asyncFunctionOverride) {
            BiConsumer<AsyncTestCommand, Callback<Double>> asyncFunction = (asyncFunctionOverride != null) ?
                    asyncFunctionOverride :
                    (i, callBack) -> executor.schedule(() ->
                            callBack.complete(Result.success(i.getValue() * i.getValue())), executionDelayMillis, TimeUnit.MILLISECONDS);

            return getAsyncContext(Constants.ASYNC_TEST_ACTION_TYPE, timeout, asyncFunction, Optional.of(Constants.ASYNC_TEST_UNDO_ACTION_TYPE));
        }

        private AsyncSpec<SpecificRecord, AsyncTestCommand, AsyncTestId, Double, AsyncTestOutput> getAsyncContext(
                String actionType,
                Optional<Duration> timeout,
                BiConsumer<AsyncTestCommand, Callback<Double>> asyncFunction,
                Optional<String> undoActionType) {
            return AsyncSpec.of(
                            actionType,
                            a -> Result.success((AsyncTestCommand) a),
                            asyncFunction,
                            "group_id",
                            Optional.of(AsyncResult.of(
                                    o -> Optional.of(Result.success(new AsyncTestOutput(o))),
                                    AsyncTestCommand::getId,
                                    (d, k, r) -> undoActionType.map(uat ->
                                            UndoCommand.of(new AsyncTestCommand(d.getId(), r.getValue()), uat)),
                                    Optional.of(asyncSerdes))),
                            timeout);
        }

        static AsyncTestContext of(int executionDelayMillis) {
            return new AsyncTestContext(executionDelayMillis, Optional.empty(), null);
        }

        static AsyncTestContext of(int executionDelayMillis, int timeoutMillis) {
            return new AsyncTestContext(executionDelayMillis, Optional.of(Duration.ofMillis(timeoutMillis)), null);
        }

        static AsyncTestContext of(BiConsumer<AsyncTestCommand, Callback<Double>> asyncFunction) {
            return new AsyncTestContext(0, Optional.empty(), asyncFunction);
        }
    }

    private static ActionRequest<SpecificRecord> createRequest(AsyncTestCommand asyncTestCommand, CommandId commandId) {
        return createRequest(asyncTestCommand, commandId, Constants.ASYNC_TEST_ACTION_TYPE, false);
    }

    private static ActionRequest<SpecificRecord> createRequest(AsyncTestCommand asyncTestCommand, CommandId commandId, String actionType, Boolean isUndo) {
        ActionCommand<SpecificRecord> actionCommand = ActionCommand.of(commandId, asyncTestCommand, actionType);
        return ActionRequest.of(
                SagaId.random(),
                ActionId.random(),
                actionCommand,
                isUndo);
    }


    @Value
    private static class ValidationRecord<K, V> {
        final K key;
        final V value;
    }

    @Value
    private static class AsyncValidation {
        final List<ValidationRecord<SagaId, ActionResponse<SpecificRecord>>> responseRecords = new ArrayList<>();
        final List<ValidationRecord<AsyncTestId, AsyncTestOutput>> outputRecords = new ArrayList<>();
        final String responseTopic = TopicNamer.forPrefix(Constants.ACTION_TOPIC_PREFIX, TopicUtils.actionTopicBaseName(Constants.ASYNC_TEST_ACTION_TYPE))
                .apply(TopicTypes.ActionTopic.ACTION_RESPONSE);

        private final RecordPublisher<SagaId, ActionResponse<SpecificRecord>> actionResponsePublisher;
        final AsyncPublisher<SagaId, ActionResponse<SpecificRecord>> responseProducer;

        final Function<TopicSerdes<AsyncTestId, AsyncTestOutput>, AsyncPublisher<AsyncTestId, AsyncTestOutput>> outputProducer;

        AsyncValidation(RecordPublisher<SagaId, ActionResponse<SpecificRecord>> actionResponsePublisher) {
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
        static AsyncValidation create(RecordPublisher<SagaId, ActionResponse<SpecificRecord>> actionResponsePublisher) { return new AsyncValidation(actionResponsePublisher);}
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

        AsyncTestCommand accountCommand = new AsyncTestCommand(new AsyncTestId("id"), 12.0);

        ActionRequest<SpecificRecord> actionRequest = createRequest(accountCommand, CommandId.random());

        acc.actionRequestPublisher.publish(actionRequest.sagaId, actionRequest);

        acc.actionUnprocessedRequestVerifier.verifySingle((id, req) -> {
            assertThat(id).isEqualTo(actionRequest.sagaId);
            assertThat(req).isEqualToComparingFieldByField(actionRequest);
        });
    }

    @Test
    void topicTypes() {
        AsyncTestContext acc = AsyncTestContext.of(100);

        assertThat(acc.expectedTopics).containsExactlyInAnyOrder(
                "saga_action_processor-saga_action-async_action_test-action_response",
                "saga_action_processor-saga_action-async_action_test-action_request",
                "saga_action_processor-saga_action-async_action_test-action_request_unprocessed",
                "async_test_topic");
    }

    @Test
    void publishesResponseAndOutput() {
        AsyncTestContext acc = AsyncTestContext.of(100);

        AsyncValidation validation = AsyncValidation.create();

        AsyncTestCommand accountCommand = new AsyncTestCommand(new AsyncTestId("id"), 12.0);
        ActionRequest<SpecificRecord> actionRequest = createRequest(new AsyncTestCommand(new AsyncTestId("id"), 12.0),CommandId.random());
        acc.actionRequestPublisher.publish(actionRequest.sagaId, actionRequest);

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
        AsyncTestCommand accountCommand = new AsyncTestCommand(testId, 12.0);

        ActionRequest<SpecificRecord> actionRequest = createRequest(accountCommand, CommandId.random());
        acc.actionRequestPublisher.publish(actionRequest.sagaId, actionRequest);

        AsyncValidation validation = AsyncValidation.create();
        AsyncActionProcessorProxy.processRecord(acc.asyncContext, actionRequest.sagaId, actionRequest, validation.responseProducer, validation.outputProducer);

        assertThat(validation.outputRecords).hasSize(0);
        assertThat(validation.responseRecords).hasSize(0);
    }

    @Test
    void timeoutLargeEnough() {
        AsyncTestContext acc = AsyncTestContext.of(200, 300);

        AsyncTestId testId = new AsyncTestId("id");
        AsyncTestCommand accountCommand = new AsyncTestCommand(testId, 12.0);

        ActionRequest<SpecificRecord> actionRequest = createRequest(accountCommand, CommandId.random());
        acc.actionRequestPublisher.publish(actionRequest.sagaId, actionRequest);

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
        AsyncTestCommand accountCommand = new AsyncTestCommand(testId, 12.0);

        ActionRequest<SpecificRecord> actionRequest = createRequest(accountCommand, CommandId.random());
        acc.actionRequestPublisher.publish(actionRequest.sagaId, actionRequest);

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
        AsyncTestCommand accountCommand = new AsyncTestCommand(testId, 12.0);

        ActionRequest<SpecificRecord> actionRequest = createRequest(accountCommand, CommandId.random());
        acc.actionRequestPublisher.publish(actionRequest.sagaId, actionRequest);

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
        AsyncTestCommand accountCommand = new AsyncTestCommand(testId, 12.0);

        ActionRequest<SpecificRecord> actionRequest = createRequest(accountCommand, CommandId.random());
        acc.actionRequestPublisher.publish(actionRequest.sagaId, actionRequest);

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

        ActionRequest<SpecificRecord> actionRequest = createRequest(new AsyncTestCommand(new AsyncTestId("id"), 12.0), CommandId.random());
        acc.actionRequestPublisher.publish(actionRequest.sagaId, actionRequest);

        acc.actionUnprocessedRequestVerifier.verifySingle((id, req) -> { });

        AsyncActionProcessorProxy.processRecord(acc.asyncContext, actionRequest.sagaId, actionRequest, validation.responseProducer, validation.outputProducer);

        delayMillis(200);
        assertThat(validation.responseRecords).hasSize(1);

        acc.actionRequestPublisher.publish(actionRequest.sagaId, actionRequest);

        // does not generate an additional request
        acc.actionUnprocessedRequestVerifier.verifyNoRecords();
    }

    @Test
    void returnsNoUndoCommandIfInUndo() {
        returnsAnUndoCommand(true);
    }


    @Test
    void returnsUndoCommand() {
        returnsAnUndoCommand(false);
    }

    void returnsAnUndoCommand(boolean isUndo) {
        AsyncTestContext acc = AsyncTestContext.of(100);

        AsyncValidation validation = AsyncValidation.create();

        ActionRequest<SpecificRecord> actionRequest = createRequest(
                new AsyncTestCommand(new AsyncTestId("id"), 12.0),
                CommandId.random(),
                Constants.ASYNC_TEST_ACTION_TYPE,
                isUndo);
        acc.actionRequestPublisher.publish(actionRequest.sagaId, actionRequest);

        AsyncActionProcessorProxy.processRecord(acc.asyncContext, actionRequest.sagaId, actionRequest, validation.responseProducer, validation.outputProducer);

        delayMillis(200);
        assertThat(validation.responseRecords).hasSize(1);
        ValidationRecord<SagaId, ActionResponse<SpecificRecord>> record = validation.responseRecords.get(0);
        assertThat(record.key).isEqualTo(actionRequest.sagaId);
        assertThat(record.value.result.isSuccess()).isTrue();
        Optional<UndoCommand<SpecificRecord>> undoCommandOpt = record.value.result.getOrElse(null);

        if (isUndo) {
            assertThat(undoCommandOpt.isPresent()).isFalse();
        } else {
            assertThat(undoCommandOpt.isPresent()).isTrue();
            UndoCommand<SpecificRecord> undoCommand = undoCommandOpt.orElse(null);

            AsyncTestCommand undoParams = new AsyncTestCommand(new AsyncTestId("id"), 144.0);
            assertThat(undoCommand.command).isEqualTo(undoParams);
            assertThat(undoCommand.actionType).isEqualTo(Constants.ASYNC_TEST_UNDO_ACTION_TYPE);
        }
    }
}
