package io.simplesource.saga.saga.app;

import io.simplesource.api.CommandId;
import io.simplesource.data.NonEmptyList;
import io.simplesource.data.Result;
import io.simplesource.kafka.spec.WindowSpec;
import io.simplesource.saga.client.dsl.SagaDSL;
import io.simplesource.saga.model.action.*;
import io.simplesource.saga.model.messages.*;
import io.simplesource.saga.model.saga.*;
import io.simplesource.saga.model.serdes.ActionSerdes;
import io.simplesource.saga.model.serdes.SagaSerdes;
import io.simplesource.saga.model.specs.ActionSpec;
import io.simplesource.saga.model.specs.SagaSpec;
import io.simplesource.saga.saga.app.SagaApp;
import io.simplesource.saga.saga.app.RetryPublisher;
import io.simplesource.saga.saga.avro.generated.test.AddFunds;
import io.simplesource.saga.saga.avro.generated.test.CreateAccount;
import io.simplesource.saga.saga.avro.generated.test.CreateUser;
import io.simplesource.saga.saga.avro.generated.test.TransferFunds;
import io.simplesource.saga.serialization.avro.AvroSerdes;
import io.simplesource.saga.shared.topics.TopicNamer;
import io.simplesource.saga.shared.topics.TopicTypes;
import io.simplesource.saga.shared.topics.TopicUtils;
import io.simplesource.saga.testutils.*;
import lombok.Value;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.streams.Topology;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.*;

import static io.simplesource.saga.client.dsl.SagaDSL.inParallel;
import static org.assertj.core.api.Assertions.assertThat;

class SagaStreamTests {

    private static String SCHEMA_URL = "http://localhost:8081/";


    @Value
    private static class TestRetryPublisher implements RetryPublisher<SpecificRecord> {
        Map<String, Map<SagaId, SagaStateTransition<SpecificRecord>>> retryMap = new HashMap<>();
        private final TopicNamer topicNamer;

        TestRetryPublisher(TopicNamer topicNamer) {

            this.topicNamer = topicNamer;
        }

        @Override
        public void send(String topic, SagaId key, SagaStateTransition<SpecificRecord> value) {
            retryMap.computeIfAbsent(topic, t -> new HashMap<>());
            retryMap.computeIfPresent(topic, (t, map) -> {
                map.put(key, value);
                return map;
            });
        }

        public SagaStateTransition<SpecificRecord> getTransition(String actionType, SagaId sagaId) {
            Map<SagaId, SagaStateTransition<SpecificRecord>>  map = retryMap.get(topicNamer.apply(TopicTypes.SagaTopic.SAGA_STATE_TRANSITION));
             return map == null ? null : map.get(sagaId);
        }

    }

    private static void delayMillis(int millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
        }
    }

    @Value
    private static class SagaCoordinatorContext {
        final TestContext testContext;
        // serdes
        final SagaSerdes<SpecificRecord> sagaSerdes = AvroSerdes.Specific.sagaSerdes(SCHEMA_URL, true);
        final ActionSerdes<SpecificRecord> actionSerdes = AvroSerdes.Specific.actionSerdes(SCHEMA_URL, true);

        // publishers
        final RecordPublisher<SagaId, SagaRequest<SpecificRecord>> sagaRequestPublisher;
        final RecordPublisher<SagaId, ActionResponse<SpecificRecord>> accountActionResponsePublisher;
        final RecordPublisher<SagaId, ActionResponse<SpecificRecord>> userActionResponsePublisher;
        final RecordPublisher<SagaId, SagaStateTransition<SpecificRecord>> sagaStateTransitionPublisher;

        // verifiers
        final RecordVerifier<SagaId, ActionRequest<SpecificRecord>> accountActionRequestVerifier;
        final RecordVerifier<SagaId, ActionRequest<SpecificRecord>> userActionRequestVerifier;
        final RecordVerifier<SagaId, SagaStateTransition<SpecificRecord>> sagaStateTransitionVerifier;
        final RecordVerifier<SagaId, Saga<SpecificRecord>> sagaStateVerifier;
        final RecordVerifier<SagaId, SagaResponse> sagaResponseVerifier;
        final List<String> topicsToCreate = new ArrayList<>();
        final TestRetryPublisher testRetryPublisher;

        SagaCoordinatorContext() {
            this(0);
        }

        SagaCoordinatorContext(int numberOfRetries) {
            TopicNamer sagaTopicNamer = TopicNamer.forPrefix(Constants.SAGA_TOPIC_PREFIX, TopicTypes.SagaTopic.SAGA_BASE_NAME);
            TopicNamer accountActionTopicNamer = TopicNamer.forPrefix(Constants.ACTION_TOPIC_PREFIX, TopicUtils.actionTopicBaseName(Constants.ACCOUNT_ACTION_TYPE));
            TopicNamer userActionTopicNamer = TopicNamer.forPrefix(Constants.ACTION_TOPIC_PREFIX, TopicUtils.actionTopicBaseName(Constants.USER_ACTION_TYPE));

            SagaApp<SpecificRecord> sagaApp = SagaApp.of(
                    SagaSpec.of(sagaSerdes, new WindowSpec(60)),
                    ActionSpec.of(actionSerdes),
                    topicBuilder -> topicBuilder.withTopicPrefix(Constants.SAGA_TOPIC_PREFIX))
                    .withAction(
                            Constants.ACCOUNT_ACTION_TYPE,
                            topicBuilder -> topicBuilder.withTopicPrefix(Constants.ACTION_TOPIC_PREFIX))
                    .withAction(
                            Constants.USER_ACTION_TYPE,
                            topicBuilder -> topicBuilder.withTopicPrefix(Constants.ACTION_TOPIC_PREFIX))
                    .withRetryStrategy(Constants.ACCOUNT_ACTION_TYPE, RetryStrategy.repeat(numberOfRetries, Duration.ofMillis(200)));

            testRetryPublisher = new TestRetryPublisher(sagaTopicNamer);

            Topology topology = sagaApp.buildTopology(topics -> topics.forEach(t -> topicsToCreate.add(t.topicName)), testRetryPublisher);
            testContext = TestContextBuilder.of(topology).build();

            sagaRequestPublisher = testContext.publisher(
                    sagaTopicNamer.apply(TopicTypes.SagaTopic.SAGA_REQUEST),
                    sagaSerdes.sagaId(),
                    sagaSerdes.request());
            accountActionResponsePublisher = testContext.publisher(
                    accountActionTopicNamer.apply(TopicTypes.ActionTopic.ACTION_RESPONSE),
                    actionSerdes.sagaId(),
                    actionSerdes.response());
            userActionResponsePublisher = testContext.publisher(
                    userActionTopicNamer.apply(TopicTypes.ActionTopic.ACTION_RESPONSE),
                    actionSerdes.sagaId(),
                    actionSerdes.response());
            sagaStateTransitionPublisher = testContext.publisher(
                    sagaTopicNamer.apply(TopicTypes.SagaTopic.SAGA_STATE_TRANSITION),
                    sagaSerdes.sagaId(),
                    sagaSerdes.transition());

            accountActionRequestVerifier = testContext.verifier(
                    accountActionTopicNamer.apply(TopicTypes.ActionTopic.ACTION_REQUEST),
                    actionSerdes.sagaId(),
                    actionSerdes.request());
            userActionRequestVerifier = testContext.verifier(
                    userActionTopicNamer.apply(TopicTypes.ActionTopic.ACTION_REQUEST),
                    actionSerdes.sagaId(),
                    actionSerdes.request());
            sagaStateTransitionVerifier = testContext.verifier(
                    sagaTopicNamer.apply(TopicTypes.SagaTopic.SAGA_STATE_TRANSITION),
                    sagaSerdes.sagaId(),
                    sagaSerdes.transition());
            sagaStateVerifier = testContext.verifier(
                    sagaTopicNamer.apply(TopicTypes.SagaTopic.SAGA_STATE),
                    sagaSerdes.sagaId(),
                    sagaSerdes.state());
            sagaResponseVerifier = testContext.verifier(
                    sagaTopicNamer.apply(TopicTypes.SagaTopic.SAGA_RESPONSE),
                    sagaSerdes.sagaId(),
                    sagaSerdes.response());
        }
    }

    private ActionId createUserId = ActionId.random();
    private ActionId createAccountId = ActionId.random();
    private ActionId addFundsId1 = ActionId.random();
    private ActionId addFundsId2 = ActionId.random();
    private ActionId transferFundsId = ActionId.random();

    Saga<SpecificRecord> getBasicSaga() {
        SagaDSL.SagaBuilder<SpecificRecord> builder = SagaDSL.createBuilder();

        SagaDSL.SubSaga<SpecificRecord> createAccount = builder.addAction(
                createAccountId,
                Constants.ACCOUNT_ACTION_TYPE,
                new CreateAccount("id1", "User 1"));

        SagaDSL.SubSaga<SpecificRecord> addFunds = builder.addAction(
                addFundsId1,
                Constants.ACCOUNT_ACTION_TYPE,
                new AddFunds("id1", 1000.0),
                // this will never undo since it's in the last sub-saga
                new AddFunds("id1", -1000.0));

        createAccount.andThen(addFunds);

        Result<SagaError, Saga<SpecificRecord>> sagaBuildResult = builder.build();
        assertThat(sagaBuildResult.isSuccess()).isEqualTo(true);

        return sagaBuildResult.getOrElse(null);
    }

    Saga<SpecificRecord> getSagaWithUndo() {
        SagaDSL.SagaBuilder<SpecificRecord> builder = SagaDSL.createBuilder();

        SagaDSL.SubSaga<SpecificRecord> addFunds = builder.addAction(
                addFundsId1,
                Constants.ACCOUNT_ACTION_TYPE,
                new AddFunds("id1", 1000.0),
                new AddFunds("id1", -1000.0));

        SagaDSL.SubSaga<SpecificRecord> transferFunds = builder.addAction(
                transferFundsId,
                Constants.ACCOUNT_ACTION_TYPE,
                new TransferFunds("id1", "id2", 50.0),
                new TransferFunds("id2", "id1", -50.0));

        addFunds.andThen(transferFunds);

        Result<SagaError, Saga<SpecificRecord>> sagaBuildResult = builder.build();
        assertThat(sagaBuildResult.isSuccess()).isEqualTo(true);

        Saga<SpecificRecord> saga = sagaBuildResult.getOrElse(null);
        return saga;
    }

    @Value
    static class CommandIds {
        CommandId action;
        CommandId undoAction;
    }

    Map<ActionId, CommandIds> getCommandIds(Saga<SpecificRecord> saga) {
        HashMap<ActionId, CommandIds> commandIdsHashMap = new HashMap<>();
        for (SagaAction<SpecificRecord> a : saga.actions.values()) {
            commandIdsHashMap.put(a.actionId, new CommandIds(a.command.commandId, a.undoCommand.map(c -> c.commandId).orElse(null)));
        }
        return commandIdsHashMap;
    }

    Saga<SpecificRecord> getParallelSaga() {
        SagaDSL.SagaBuilder<SpecificRecord> builder = SagaDSL.createBuilder();

        SagaDSL.SubSaga<SpecificRecord> addFunds1 = builder.addAction(
                addFundsId1,
                Constants.ACCOUNT_ACTION_TYPE,
                new AddFunds("id1", 1000.0),
                new AddFunds("id1", -1000.0));
        SagaDSL.SubSaga<SpecificRecord> addFunds2 = builder.addAction(
                addFundsId2,
                Constants.ACCOUNT_ACTION_TYPE,
                new AddFunds("id2", 1000.0),
                new AddFunds("id2", -1000.0));

        inParallel(addFunds1, addFunds2);

        Result<SagaError, Saga<SpecificRecord>> sagaBuildResult = builder.build();
        assertThat(sagaBuildResult.isSuccess()).isEqualTo(true);

        return sagaBuildResult.getOrElse(null);
    }

    Saga<SpecificRecord> getParallelSaga3Actions() {
        SagaDSL.SagaBuilder<SpecificRecord> builder = SagaDSL.createBuilder();

        SagaDSL.SubSaga<SpecificRecord> addFunds1 = builder.addAction(
                addFundsId1,
                Constants.ACCOUNT_ACTION_TYPE,
                new AddFunds("id1", 1000.0),
                new AddFunds("id1", -1000.0));
        SagaDSL.SubSaga<SpecificRecord> addFunds2 = builder.addAction(
                addFundsId2,
                Constants.ACCOUNT_ACTION_TYPE,
                new AddFunds("id2", 1000.0),
                new AddFunds("id2", -1000.0));
        SagaDSL.SubSaga<SpecificRecord> transferFunds = builder.addAction(
                transferFundsId,
                Constants.ACCOUNT_ACTION_TYPE,
                new TransferFunds("id3", "id4", 10.0),
                new TransferFunds("id4", "id3", 10.0));

        inParallel(addFunds1, addFunds2, transferFunds);

        Result<SagaError, Saga<SpecificRecord>> sagaBuildResult = builder.build();
        assertThat(sagaBuildResult.isSuccess()).isEqualTo(true);

        return sagaBuildResult.getOrElse(null);
    }

    Saga<SpecificRecord> getInvalidActionSaga() {
        SagaDSL.SagaBuilder<SpecificRecord> builder = SagaDSL.createBuilder();

        SagaDSL.SubSaga<SpecificRecord> createAccount = builder.addAction(
                createAccountId,
                Constants.ACCOUNT_ACTION_TYPE,
                new CreateAccount("id1", "User 1"));

        SagaDSL.SubSaga<SpecificRecord> addFunds1 = builder.addAction(
                addFundsId1,
                "invalid action type 1",
                new AddFunds("id1", 1000.0),
                // this will never undo since it's in the last sub-saga
                new AddFunds("id1", -1000.0));

        SagaDSL.SubSaga<SpecificRecord> addFunds2 = builder.addAction(
                addFundsId2,
                "invalid action type 2",
                new AddFunds("id1", 1000.0),
                // this will never undo since it's in the last sub-saga
                new AddFunds("id1", -1000.0));

        createAccount.andThen(addFunds1).andThen(addFunds2);

        Result<SagaError, Saga<SpecificRecord>> sagaBuildResult = builder.build();
        assertThat(sagaBuildResult.isSuccess()).isEqualTo(true);

        return sagaBuildResult.getOrElse(null);
    }

    Saga<SpecificRecord> getMultiActionTypeSaga() {
        SagaDSL.SagaBuilder<SpecificRecord> builder = SagaDSL.createBuilder();

        SagaDSL.SubSaga<SpecificRecord> createUser = builder.addAction(
                createUserId,
                Constants.USER_ACTION_TYPE,
                new CreateUser("First", "Last", 2000));

        SagaDSL.SubSaga<SpecificRecord> createAccount = builder.addAction(
                createAccountId,
                Constants.ACCOUNT_ACTION_TYPE,
                new CreateAccount("id1", "User 1"));

        createUser.andThen(createAccount);

        Result<SagaError, Saga<SpecificRecord>> sagaBuildResult = builder.build();
        assertThat(sagaBuildResult.isSuccess()).isEqualTo(true);

        return sagaBuildResult.getOrElse(null);
    }

    @Test
    void testTopicCreation() {
        SagaCoordinatorContext scc = new SagaCoordinatorContext();

        assertThat(scc.topicsToCreate).containsExactlyInAnyOrder(
                "saga_coordinator-saga_coordinator-saga_state",
                "saga_coordinator-saga_coordinator-saga_response_topic_map",
                "saga_coordinator-saga_coordinator-saga_request",
                "saga_coordinator-saga_coordinator-saga_response",
                "saga_coordinator-saga_coordinator-saga_state_transition",
                "saga_action_processor-saga_action-sourcing_action_account-action_response",
                "saga_action_processor-saga_action-sourcing_action_account-action_request",
                "saga_action_processor-saga_action-sourcing_action_user-action_response",
                "saga_action_processor-saga_action-sourcing_action_user-action_request");
    }

    @Test
    void testSuccessfulSaga() {
        SagaCoordinatorContext scc = new SagaCoordinatorContext();

        SagaId sagaRequestId = SagaId.random();
        Saga<SpecificRecord> saga = getBasicSaga();
        Map<ActionId, CommandIds> commandIds = getCommandIds(saga);

        scc.sagaRequestPublisher.publish(saga.sagaId, SagaRequest.of(sagaRequestId, saga));

        scc.accountActionRequestVerifier.verifySingle((id, actionRequest) -> {
            assertThat(id).isEqualTo(saga.sagaId);
            assertThat(actionRequest.actionId).isEqualTo(createAccountId);
            assertThat(actionRequest.actionCommand.actionType).isEqualTo(Constants.ACCOUNT_ACTION_TYPE);
            assertThat(actionRequest.actionCommand.commandId).isEqualTo(commandIds.get(createAccountId).action);
        });

        scc.sagaStateTransitionVerifier.verifyMultiple(2, (i, id, stateTransition) -> {
            if (i == 0) {
                assertThat(stateTransition).isInstanceOf(SagaStateTransition.SetInitialState.class);
                Saga s = ((SagaStateTransition.SetInitialState) stateTransition).sagaState;
                assertThat(s.status).isEqualTo(SagaStatus.NotStarted);
                assertThat(s.sequence.getSeq()).isEqualTo(0);
                assertThat(s.actions).containsKeys(createAccountId, addFundsId1);
            } else if (i == 1) {
                assertThat(stateTransition).isInstanceOf(SagaStateTransition.TransitionList.class);
                SagaStateTransition.TransitionList<SpecificRecord> transitionList = (SagaStateTransition.TransitionList<SpecificRecord>) stateTransition;
                assertThat(transitionList.actions.size()).isEqualTo(1);
                assertThat(transitionList.actions.get(0).actionStatus).isEqualTo(ActionStatus.InProgress);
            }
        });

        scc.sagaStateVerifier.verifyMultiple(2, (i, id, state) -> {
            if (i == 0) {
                assertThat(state.sequence.getSeq()).isEqualTo(0);
                assertThat(state.status).isEqualTo(SagaStatus.InProgress);
                assertThat(state.actions.get(createAccountId).status).isEqualTo(ActionStatus.Pending);
                assertThat(state.actions.get(addFundsId1).status).isEqualTo(ActionStatus.Pending);
            } else if (i == 1) {
                assertThat(state.sequence.getSeq()).isEqualTo(1);
                assertThat(state.status).isEqualTo(SagaStatus.InProgress);
                assertThat(state.actions.get(createAccountId).status).isEqualTo(ActionStatus.InProgress);
                assertThat(state.actions.get(addFundsId1).status).isEqualTo(ActionStatus.Pending);
            }
        });

        // create account successful
        scc.accountActionResponsePublisher.publish(
                saga.sagaId,
                ActionResponse.of(
                        saga.sagaId, createAccountId,
                        commandIds.get(createAccountId).action,
                        false,
                        Result.success(Optional.empty())));

        scc.sagaStateTransitionVerifier.verifyMultiple(2, (i, id, stateTransition) -> {
            if (i == 0) {
                assertThat(stateTransition).isInstanceOf(SagaStateTransition.SagaActionStateChanged.class);
                SagaStateTransition.SagaActionStateChanged<?> c = ((SagaStateTransition.SagaActionStateChanged) stateTransition);
                assertThat(c.actionId).isEqualTo(createAccountId);
                assertThat(c.actionStatus).isEqualTo(ActionStatus.Completed);
            } else if (i == 1) {
                assertThat(stateTransition).isInstanceOf(SagaStateTransition.TransitionList.class);
                SagaStateTransition.TransitionList<SpecificRecord> transitionList = (SagaStateTransition.TransitionList<SpecificRecord>) stateTransition;
                assertThat(transitionList.actions.size()).isEqualTo(1);
                assertThat(transitionList.actions.get(0).actionStatus).isEqualTo(ActionStatus.InProgress);
            }
        });

        scc.sagaStateVerifier.verifyMultiple(2, (i, id, state) -> {
            if (i == 0) {
                assertThat(state.sequence.getSeq()).isEqualTo(2);
                assertThat(state.status).isEqualTo(SagaStatus.InProgress);
                assertThat(state.actions.get(createAccountId).status).isEqualTo(ActionStatus.Completed);
                assertThat(state.actions.get(addFundsId1).status).isEqualTo(ActionStatus.Pending);
            } else if (i == 1) {
                assertThat(state.sequence.getSeq()).isEqualTo(3);
                assertThat(state.status).isEqualTo(SagaStatus.InProgress);
                assertThat(state.actions.get(createAccountId).status).isEqualTo(ActionStatus.Completed);
                assertThat(state.actions.get(addFundsId1).status).isEqualTo(ActionStatus.InProgress);
            }
        });

        scc.accountActionRequestVerifier.verifySingle((id, actionRequest) -> {
            assertThat(id).isEqualTo(saga.sagaId);
            assertThat(actionRequest.sagaId).isEqualTo(saga.sagaId);
            assertThat(actionRequest.actionId).isEqualTo(addFundsId1);
            assertThat(actionRequest.actionCommand.actionType).isEqualTo(Constants.ACCOUNT_ACTION_TYPE);
            assertThat(actionRequest.actionCommand.commandId).isEqualTo(commandIds.get(addFundsId1).action);
        });

        // add funds successful
        UndoCommand<SpecificRecord> dynamicUndo =
                UndoCommand.of(new AddFunds("id1", -999.0), "");
        scc.accountActionResponsePublisher.publish(
                saga.sagaId,
                ActionResponse.of(saga.sagaId, addFundsId1, commandIds.get(addFundsId1).action,
                        false,
                        Result.success(Optional.of(dynamicUndo))));

        scc.sagaStateTransitionVerifier.verifyMultiple(2, (i, id, stateTransition) -> {
            if (i == 0) {
                assertThat(stateTransition).isInstanceOf(SagaStateTransition.SagaActionStateChanged.class);
                SagaStateTransition.SagaActionStateChanged<SpecificRecord> c =
                        (SagaStateTransition.SagaActionStateChanged<SpecificRecord>) stateTransition;
                assertThat(c.actionId).isEqualTo(addFundsId1);
                assertThat(c.actionStatus).isEqualTo(ActionStatus.Completed);
                assertThat(c.undoCommand).isEqualTo(Optional.of(dynamicUndo));
            } else if (i == 1) {
                assertThat(stateTransition).isInstanceOf(SagaStateTransition.SagaStatusChanged.class);
                SagaStateTransition.SagaStatusChanged<?> c = ((SagaStateTransition.SagaStatusChanged) stateTransition);
                assertThat(c.sagaId).isEqualTo(saga.sagaId);
                assertThat(c.sagaStatus).isEqualTo(SagaStatus.Completed);
            }
        });

        scc.sagaStateVerifier.verifyMultiple(2, (i, id, state) -> {
            if (i == 0) {
                assertThat(state.sequence.getSeq()).isEqualTo(4);
                assertThat(state.status).isEqualTo(SagaStatus.InProgress);
                assertThat(state.actions.get(createAccountId).status).isEqualTo(ActionStatus.Completed);
                assertThat(state.actions.get(addFundsId1).status).isEqualTo(ActionStatus.Completed);
            } else if (i == 1) {
                assertThat(state.sequence.getSeq()).isEqualTo(5);
                assertThat(state.status).isEqualTo(SagaStatus.Completed);
                assertThat(state.actions.get(createAccountId).status).isEqualTo(ActionStatus.Completed);
                SagaAction<SpecificRecord> newAddFunds1 = state.actions.get(addFundsId1);
                assertThat(newAddFunds1.status).isEqualTo(ActionStatus.Completed);
                assertThat(newAddFunds1.undoCommand.map(c -> c.command)).isEqualTo(Optional.of(dynamicUndo).map(c -> c.command));
            }
        });
        scc.accountActionRequestVerifier.verifyNoRecords();

        scc.sagaResponseVerifier.verifySingle((id, response) -> {
            assertThat(response.sagaId).isEqualTo(saga.sagaId);
            assertThat(response.result.isSuccess()).isTrue();
        });
    }

    @Test
    void testShortCircuitOnFailure() {
        SagaCoordinatorContext scc = new SagaCoordinatorContext();

        SagaId sagaRequestId = SagaId.random();
        Saga<SpecificRecord> saga = getBasicSaga();
        Map<ActionId, CommandIds> commandIds = getCommandIds(saga);
        scc.sagaRequestPublisher.publish(saga.sagaId, SagaRequest.of(sagaRequestId, saga));

        // already verified in above test
        scc.accountActionRequestVerifier.drainAll();
        scc.sagaStateTransitionVerifier.drainAll();
        scc.sagaStateVerifier.drainAll();

        // create account failed
        SagaError sagaError = SagaError.of(SagaError.Reason.CommandError, "Oh noes");
        scc.accountActionResponsePublisher.publish(saga.sagaId, ActionResponse.of(saga.sagaId,
                createAccountId,
                commandIds.get(createAccountId).action,
                false,
                Result.failure(sagaError)));

        verifySagaFailed(scc, saga, sagaError, 2);
    }

    private void verifySagaFailed(SagaCoordinatorContext scc, Saga<SpecificRecord> saga, SagaError sagaError, int firstSeq) {
        scc.sagaStateTransitionVerifier.verifyMultiple(2, (i, id, stateTransition) -> {
            if (i == 0) {
                assertThat(stateTransition).isInstanceOf(SagaStateTransition.SagaActionStateChanged.class);
                SagaStateTransition.SagaActionStateChanged<?> c = ((SagaStateTransition.SagaActionStateChanged) stateTransition);
                assertThat(c.actionId).isEqualTo(createAccountId);
                assertThat(c.actionStatus).isEqualTo(ActionStatus.Failed);
            } else if (i == 1) {
                assertThat(stateTransition).isInstanceOf(SagaStateTransition.SagaStatusChanged.class);
                SagaStateTransition.SagaStatusChanged<?> c = ((SagaStateTransition.SagaStatusChanged) stateTransition);
                assertThat(c.sagaStatus).isEqualTo(SagaStatus.Failed);
            }
        });

        scc.sagaStateVerifier.verifyMultiple(2, (i, id, state) -> {
            if (i == 0) {
                assertThat(state.sequence.getSeq()).isEqualTo(firstSeq);
                assertThat(state.status).isEqualTo(SagaStatus.InProgress);
                assertThat(state.actions.get(createAccountId).status).isEqualTo(ActionStatus.Failed);
                assertThat(state.actions.get(addFundsId1).status).isEqualTo(ActionStatus.Pending);
            } else if (i == 1) {
                assertThat(state.sequence.getSeq()).isEqualTo(firstSeq + 1);
                assertThat(state.status).isEqualTo(SagaStatus.Failed);
                assertThat(state.actions.get(createAccountId).status).isEqualTo(ActionStatus.Failed);
                assertThat(state.actions.get(addFundsId1).status).isEqualTo(ActionStatus.Pending);
            }
        });

        scc.accountActionRequestVerifier.verifyNoRecords();

        scc.sagaResponseVerifier.verifySingle((id, response) -> {
            assertThat(response.sagaId).isEqualTo(saga.sagaId);
            assertThat(response.result.isSuccess()).isFalse();
            assertThat(response.result.failureReasons()).contains(NonEmptyList.of(sagaError));
        });
    }

    @Test
    void testRetryOnFailure() {
        SagaCoordinatorContext scc = new SagaCoordinatorContext(1);

        Saga<SpecificRecord> saga = getBasicSaga();
        Map<ActionId, CommandIds> commandIds = getCommandIds(saga);
        ActionCommand<SpecificRecord> createCommand = saga.actions.get(createAccountId).command;
        scc.sagaRequestPublisher.publish(saga.sagaId, SagaRequest.of(saga.sagaId, saga));

        // already verified in above test
        scc.accountActionRequestVerifier.drainAll();
        scc.sagaStateTransitionVerifier.drainAll();
        scc.sagaStateVerifier.drainAll();

        // create account failed
        SagaError sagaError = SagaError.of(SagaError.Reason.CommandError, "Oh noes");
        scc.accountActionResponsePublisher.publish(saga.sagaId, ActionResponse.of(
                saga.sagaId, createAccountId,
                commandIds.get(createAccountId).action,
                false,
                Result.failure(sagaError)));

        scc.sagaStateTransitionVerifier.verifySingle((id, stateTransition) -> {
            assertThat(stateTransition).isInstanceOf(SagaStateTransition.SagaActionStateChanged.class);
            SagaStateTransition.SagaActionStateChanged<?> c = ((SagaStateTransition.SagaActionStateChanged) stateTransition);
            assertThat(c.actionId).isEqualTo(createAccountId);
            assertThat(c.actionStatus).isEqualTo(ActionStatus.RetryAwaiting);
        });

        scc.sagaStateVerifier.verifySingle((id, state) -> {
            assertThat(state.sequence.getSeq()).isEqualTo(2);
            assertThat(state.status).isEqualTo(SagaStatus.InProgress);
            assertThat(state.actions.get(createAccountId).status).isEqualTo(ActionStatus.RetryAwaiting);
            assertThat(state.actions.get(addFundsId1).status).isEqualTo(ActionStatus.Pending);
        });

        delayMillis(300);
        SagaStateTransition<SpecificRecord> retryTransition = scc.testRetryPublisher.getTransition(Constants.ACCOUNT_ACTION_TYPE, saga.sagaId);
        assertThat(retryTransition).isInstanceOf(SagaStateTransition.SagaActionStateChanged.class);
        SagaStateTransition.SagaActionStateChanged<?> retryStateChange = ((SagaStateTransition.SagaActionStateChanged) retryTransition);
        // TODO: complete
        assertThat(retryStateChange).isEqualTo(SagaStateTransition.SagaActionStateChanged.of(
                saga.sagaId,
                createAccountId,
                ActionStatus.RetryCompleted,
                Collections.emptyList(), Optional.empty(),
                false));

        scc.accountActionRequestVerifier.verifyNoRecords();
        scc.sagaResponseVerifier.verifyNoRecords();

        // complete the async setting of action back to pending, so that it can be rerun
        scc.sagaStateTransitionPublisher.publish(saga.sagaId, retryTransition);

        scc.accountActionRequestVerifier.verifySingle((id, actionRequest) -> {
            assertThat(id).isEqualTo(saga.sagaId);
            assertThat(actionRequest.sagaId).isEqualTo(saga.sagaId);
            assertThat(actionRequest.actionId).isEqualTo(createAccountId);
            assertThat(actionRequest.actionCommand.actionType).isEqualTo(Constants.ACCOUNT_ACTION_TYPE);
            assertThat(actionRequest.actionCommand.command).isEqualTo(createCommand.command);
            assertThat(actionRequest.actionCommand.commandId).isNotEqualTo(createCommand);
        });

        scc.sagaStateVerifier.verifyMultiple(2, (i, id, state) -> {
            if (i == 0) {
                assertThat(state.sequence.getSeq()).isEqualTo(3);
                assertThat(state.status).isEqualTo(SagaStatus.InProgress);
                assertThat(state.actions.get(createAccountId).status).isEqualTo(ActionStatus.Pending);
                assertThat(state.actions.get(addFundsId1).status).isEqualTo(ActionStatus.Pending);
            } else {
                assertThat(state.sequence.getSeq()).isEqualTo(4);
                assertThat(state.status).isEqualTo(SagaStatus.InProgress);
                assertThat(state.actions.get(createAccountId).status).isEqualTo(ActionStatus.InProgress);
                assertThat(state.actions.get(addFundsId1).status).isEqualTo(ActionStatus.Pending);
            }
        });
        scc.sagaStateTransitionVerifier.drainAll();

        // create account failed
        SagaError sagaError2 = SagaError.of(SagaError.Reason.CommandError, "Oh noes 2");
        scc.accountActionResponsePublisher.publish(saga.sagaId, ActionResponse.of(
                saga.sagaId,
                createAccountId,
                commandIds.get(createAccountId).action,
                false,
                Result.failure(sagaError2)));

        verifySagaFailed(scc, saga, sagaError2, 5);
    }

    @Test
    void testBypassUndoOnFailureIfNotDefined() {
        SagaCoordinatorContext scc = new SagaCoordinatorContext();

        SagaId sagaRequestId = SagaId.random();
        Saga<SpecificRecord> saga = getBasicSaga();
        Map<ActionId, CommandIds> commandIds = getCommandIds(saga);
        scc.sagaRequestPublisher.publish(saga.sagaId, SagaRequest.of(sagaRequestId, saga));

        // create account successful
        scc.accountActionResponsePublisher.publish(saga.sagaId, ActionResponse.of(
                saga.sagaId,
                createAccountId,
                commandIds.get(createAccountId).action,
                false,
                Result.success(Optional.empty())));

        scc.accountActionRequestVerifier.drainAll();
        scc.sagaStateTransitionVerifier.drainAll();
        scc.sagaStateVerifier.drainAll();

        // add funds failed
        SagaError sagaError = SagaError.of(SagaError.Reason.CommandError, "Oh noes");
        scc.accountActionResponsePublisher.publish(saga.sagaId, ActionResponse.of(
                saga.sagaId, addFundsId1,
                commandIds.get(addFundsId1).action,
                false,
                Result.failure(sagaError)));

        scc.sagaStateTransitionVerifier.verifyMultiple(4, (i, id, stateTransition) -> {
            if (i == 0) {
                assertThat(stateTransition).isInstanceOf(SagaStateTransition.SagaActionStateChanged.class);
                SagaStateTransition.SagaActionStateChanged<?> c = ((SagaStateTransition.SagaActionStateChanged) stateTransition);
                assertThat(c.actionId).isEqualTo(addFundsId1);
                assertThat(c.actionStatus).isEqualTo(ActionStatus.Failed);
            } else if (i == 1) {
                assertThat(stateTransition).isInstanceOf(SagaStateTransition.SagaStatusChanged.class);
                SagaStateTransition.SagaStatusChanged<?> c = ((SagaStateTransition.SagaStatusChanged) stateTransition);
                assertThat(c.sagaStatus).isEqualTo(SagaStatus.InFailure);
            } else if (i == 2) {
                assertThat(stateTransition).isInstanceOf(SagaStateTransition.TransitionList.class);
                SagaStateTransition.TransitionList<SpecificRecord> transitionList = (SagaStateTransition.TransitionList<SpecificRecord>) stateTransition;
                assertThat(transitionList.actions.size()).isEqualTo(1);
                assertThat(transitionList.actions.get(0).actionStatus).isEqualTo(ActionStatus.UndoBypassed);
            } else if (i == 3) {
                assertThat(stateTransition).isInstanceOf(SagaStateTransition.SagaStatusChanged.class);
                SagaStateTransition.SagaStatusChanged<?> c = ((SagaStateTransition.SagaStatusChanged) stateTransition);
                assertThat(c.sagaStatus).isEqualTo(SagaStatus.Failed);
            }
        });

        scc.sagaStateVerifier.verifyMultiple(4, (i, id, state) -> {
            if (i == 0) {
                assertThat(state.sequence.getSeq()).isEqualTo(4);
                assertThat(state.status).isEqualTo(SagaStatus.InProgress);
                assertThat(state.actions.get(createAccountId).status).isEqualTo(ActionStatus.Completed);
                assertThat(state.actions.get(addFundsId1).status).isEqualTo(ActionStatus.Failed);
            } else if (i == 1) {
                assertThat(state.sequence.getSeq()).isEqualTo(5);
                assertThat(state.status).isEqualTo(SagaStatus.InFailure);
                assertThat(state.actions.get(createAccountId).status).isEqualTo(ActionStatus.Completed);
                assertThat(state.actions.get(addFundsId1).status).isEqualTo(ActionStatus.Failed);
            } else if (i == 2) {
                assertThat(state.sequence.getSeq()).isEqualTo(6);
                assertThat(state.status).isEqualTo(SagaStatus.InFailure);
                assertThat(state.actions.get(createAccountId).status).isEqualTo(ActionStatus.UndoBypassed);
                assertThat(state.actions.get(addFundsId1).status).isEqualTo(ActionStatus.Failed);
            } else if (i == 3) {
                assertThat(state.sequence.getSeq()).isEqualTo(7);
                assertThat(state.status).isEqualTo(SagaStatus.Failed);
                assertThat(state.actions.get(createAccountId).status).isEqualTo(ActionStatus.UndoBypassed);
                assertThat(state.actions.get(addFundsId1).status).isEqualTo(ActionStatus.Failed);
            }
        });

        scc.accountActionRequestVerifier.verifyNoRecords();

        scc.sagaResponseVerifier.verifySingle((id, response) -> {
            assertThat(response.sagaId).isEqualTo(saga.sagaId);
            assertThat(response.result.isSuccess()).isFalse();
            assertThat(response.result.failureReasons()).contains(NonEmptyList.of(sagaError));
        });
    }

    @Test
    void testUndoCommandOnFailure() {
        SagaCoordinatorContext scc = new SagaCoordinatorContext();

        SagaId sagaRequestId = SagaId.random();
        Saga<SpecificRecord> saga = getSagaWithUndo();
        Map<ActionId, CommandIds> commandIds = getCommandIds(saga);
        scc.sagaRequestPublisher.publish(saga.sagaId, SagaRequest.of(sagaRequestId, saga));

        // add funds successful
        scc.accountActionResponsePublisher.publish(saga.sagaId, ActionResponse.of(
                saga.sagaId,
                addFundsId1,
                commandIds.get(addFundsId1).action,
                false,
                Result.success(Optional.empty())));

        scc.accountActionRequestVerifier.drainAll();
        scc.sagaStateTransitionVerifier.drainAll();
        scc.sagaStateVerifier.drainAll();

        // transfer funds failed
        SagaError sagaError = SagaError.of(SagaError.Reason.CommandError, "Oh noes");
        scc.accountActionResponsePublisher.publish(saga.sagaId, ActionResponse.of(
                saga.sagaId, transferFundsId,
                commandIds.get(transferFundsId).action,
                false,
                Result.failure(sagaError)));

        scc.sagaStateTransitionVerifier.verifyMultiple(3, (i, id, stateTransition) -> {
            if (i == 0) {
                assertThat(stateTransition).isInstanceOf(SagaStateTransition.SagaActionStateChanged.class);
                SagaStateTransition.SagaActionStateChanged<?> c = ((SagaStateTransition.SagaActionStateChanged) stateTransition);
                assertThat(c.actionId).isEqualTo(transferFundsId);
                assertThat(c.actionStatus).isEqualTo(ActionStatus.Failed);
            } else if (i == 1) {
                assertThat(stateTransition).isInstanceOf(SagaStateTransition.SagaStatusChanged.class);
                SagaStateTransition.SagaStatusChanged<?> c = ((SagaStateTransition.SagaStatusChanged) stateTransition);
                assertThat(c.sagaStatus).isEqualTo(SagaStatus.InFailure);
            } else if (i == 2) {
                assertThat(stateTransition).isInstanceOf(SagaStateTransition.TransitionList.class);
                SagaStateTransition.TransitionList<SpecificRecord> transitionList = (SagaStateTransition.TransitionList<SpecificRecord>) stateTransition;
                assertThat(transitionList.actions.size()).isEqualTo(1);
                assertThat(transitionList.actions.get(0).actionStatus).isEqualTo(ActionStatus.UndoInProgress);
            }
        });

        scc.sagaStateVerifier.verifyMultiple(3, (i, id, state) -> {
            if (i == 0) {
                assertThat(state.sequence.getSeq()).isEqualTo(4);
                assertThat(state.status).isEqualTo(SagaStatus.InProgress);
                assertThat(state.actions.get(addFundsId1).status).isEqualTo(ActionStatus.Completed);
                assertThat(state.actions.get(transferFundsId).status).isEqualTo(ActionStatus.Failed);
            } else if (i == 1) {
                assertThat(state.sequence.getSeq()).isEqualTo(5);
                assertThat(state.status).isEqualTo(SagaStatus.InFailure);
                assertThat(state.actions.get(addFundsId1).status).isEqualTo(ActionStatus.Completed);
                assertThat(state.actions.get(transferFundsId).status).isEqualTo(ActionStatus.Failed);
            } else if (i == 2) {
                assertThat(state.sequence.getSeq()).isEqualTo(6);
                assertThat(state.status).isEqualTo(SagaStatus.InFailure);
                assertThat(state.actions.get(addFundsId1).status).isEqualTo(ActionStatus.UndoInProgress);
                assertThat(state.actions.get(transferFundsId).status).isEqualTo(ActionStatus.Failed);
            }
        });

        scc.accountActionRequestVerifier.verifySingle((id, actionRequest) -> {
            assertThat(id).isEqualTo(saga.sagaId);
            assertThat(actionRequest.sagaId).isEqualTo(saga.sagaId);
            assertThat(actionRequest.actionId).isEqualTo(addFundsId1);
            assertThat(actionRequest.actionCommand.actionType).isEqualTo(Constants.ACCOUNT_ACTION_TYPE);
            assertThat(actionRequest.actionCommand.commandId).isEqualTo(commandIds.get(addFundsId1).undoAction);
        });

        // undo add funds successful
        scc.accountActionResponsePublisher.publish(saga.sagaId, ActionResponse.of(
                saga.sagaId,
                addFundsId1,
                commandIds.get(addFundsId1).undoAction,
                true,
                Result.success(Optional.empty())));

        verifyFinishUndo(scc, saga, sagaError, 7);
    }

    private void verifyFinishUndo(SagaCoordinatorContext scc, Saga<SpecificRecord> saga, SagaError sagaError, int offset) {

        scc.sagaStateTransitionVerifier.verifyMultiple(2, (i, id, stateTransition) -> {
            if (i == 0) {
                assertThat(stateTransition).isInstanceOf(SagaStateTransition.SagaActionStateChanged.class);
                SagaStateTransition.SagaActionStateChanged<?> c = ((SagaStateTransition.SagaActionStateChanged) stateTransition);
                assertThat(c.actionId).isEqualTo(addFundsId1);
                assertThat(c.actionStatus).isEqualTo(ActionStatus.Undone);
            } else if (i == 1) {
                assertThat(stateTransition).isInstanceOf(SagaStateTransition.SagaStatusChanged.class);
                SagaStateTransition.SagaStatusChanged<?> c = ((SagaStateTransition.SagaStatusChanged) stateTransition);
                assertThat(c.sagaStatus).isEqualTo(SagaStatus.Failed);
            }
        });

        scc.sagaStateVerifier.verifyMultiple(2, (i, id, state) -> {
            if (i == 0) {
                assertThat(state.sequence.getSeq()).isEqualTo(offset);
                assertThat(state.status).isEqualTo(SagaStatus.InFailure);
                assertThat(state.actions.get(addFundsId1).status).isEqualTo(ActionStatus.Undone);
                assertThat(state.actions.get(transferFundsId).status).isEqualTo(ActionStatus.Failed);
            } else if (i == 1) {
                assertThat(state.sequence.getSeq()).isEqualTo(offset + 1);
                assertThat(state.status).isEqualTo(SagaStatus.Failed);
                assertThat(state.actions.get(addFundsId1).status).isEqualTo(ActionStatus.Undone);
                assertThat(state.actions.get(transferFundsId).status).isEqualTo(ActionStatus.Failed);
            }
        });

        scc.sagaResponseVerifier.verifySingle((id, response) -> {
            assertThat(response.sagaId).isEqualTo(saga.sagaId);
            assertThat(response.result.isSuccess()).isFalse();
            assertThat(response.result.failureReasons()).contains(NonEmptyList.of(sagaError));
        });
    }

    @Test
    void testRetryUndoCommandOnFailure() {
        SagaCoordinatorContext scc = new SagaCoordinatorContext(1);

        SagaId sagaRequestId = SagaId.random();
        Saga<SpecificRecord> saga = getSagaWithUndo();
        Map<ActionId, CommandIds> commandIds = getCommandIds(saga);
        scc.sagaRequestPublisher.publish(saga.sagaId, SagaRequest.of(sagaRequestId, saga));

        // add funds successful
        scc.accountActionResponsePublisher.publish(saga.sagaId, ActionResponse.of(
                saga.sagaId, addFundsId1,
                commandIds.get(addFundsId1).action,
                false,
                Result.success(Optional.empty())));

        // transfer funds failed
        SagaError sagaError1 = SagaError.of(SagaError.Reason.CommandError, "Oh noes");
        scc.accountActionResponsePublisher.publish(saga.sagaId, ActionResponse.of(
                saga.sagaId,
                transferFundsId,
                commandIds.get(transferFundsId).action,
                false,
                Result.failure(sagaError1)));

        delayMillis(300);
        // complete the async setting of action back to pending, so that it can be rerun
        SagaStateTransition<SpecificRecord> retryTransition =scc.testRetryPublisher.getTransition(Constants.ACCOUNT_ACTION_TYPE, saga.sagaId);
        scc.sagaStateTransitionPublisher.publish(saga.sagaId, retryTransition);

        // failed again
        SagaError sagaError2 = SagaError.of(SagaError.Reason.CommandError, "Oh noes again");
        scc.accountActionResponsePublisher.publish(saga.sagaId, ActionResponse.of(
                saga.sagaId,
                transferFundsId,
                commandIds.get(transferFundsId).action,
                false,
                Result.failure(sagaError2)));

        scc.sagaStateVerifier.drainAll();
        scc.accountActionRequestVerifier.drainAll();
        scc.sagaResponseVerifier.drainAll();
        scc.sagaStateTransitionVerifier.drainAll();

        // undo add funds failed
        SagaError sagaUndoError = SagaError.of(SagaError.Reason.CommandError, "Oh no undo too");
        scc.accountActionResponsePublisher.publish(saga.sagaId, ActionResponse.of(
                saga.sagaId,
                addFundsId1,
                commandIds.get(addFundsId1).undoAction,
                true,
                Result.failure(sagaUndoError)));

        scc.sagaStateTransitionVerifier.verifySingle((id, stateTransition) -> {
            assertThat(stateTransition).isInstanceOf(SagaStateTransition.SagaActionStateChanged.class);
            SagaStateTransition.SagaActionStateChanged<?> c = ((SagaStateTransition.SagaActionStateChanged) stateTransition);
            assertThat(c.actionId).isEqualTo(addFundsId1);
            assertThat(c.actionStatus).isEqualTo(ActionStatus.RetryAwaiting);
        });

        scc.sagaStateVerifier.verifySingle((id, state) -> {
                assertThat(state.sequence.getSeq()).isEqualTo(10);
                assertThat(state.status).isEqualTo(SagaStatus.InFailure);
                assertThat(state.actions.get(addFundsId1).status).isEqualTo(ActionStatus.RetryAwaiting);
                assertThat(state.actions.get(transferFundsId).status).isEqualTo(ActionStatus.Failed);
        });

        delayMillis(300);
        // complete the async setting of action back to pending, so that it can be rerun
        SagaStateTransition<SpecificRecord> retryUndoTransition = scc.testRetryPublisher.getTransition(Constants.ACCOUNT_ACTION_TYPE, saga.sagaId);
        assertThat(retryUndoTransition).isInstanceOf(SagaStateTransition.SagaActionStateChanged.class);
        SagaStateTransition.SagaActionStateChanged<?> retryStateChange = ((SagaStateTransition.SagaActionStateChanged) retryUndoTransition);
        assertThat(retryStateChange).isEqualTo(SagaStateTransition.SagaActionStateChanged.of(
                saga.sagaId,
                addFundsId1,
                ActionStatus.RetryCompleted,
                Collections.emptyList(),
                Optional.empty(),
                true));


        scc.sagaStateTransitionPublisher.publish(saga.sagaId, retryUndoTransition);

        scc.sagaStateVerifier.verifyMultiple(2, (i, id, state) -> {
            if (i == 0) {
                assertThat(state.sequence.getSeq()).isEqualTo(11);
                assertThat(state.status).isEqualTo(SagaStatus.InFailure);
                assertThat(state.actions.get(addFundsId1).status).isEqualTo(ActionStatus.Completed);
                assertThat(state.actions.get(transferFundsId).status).isEqualTo(ActionStatus.Failed);
            }
            if (i == 1) {
                assertThat(state.sequence.getSeq()).isEqualTo(12);
                assertThat(state.status).isEqualTo(SagaStatus.InFailure);
                assertThat(state.actions.get(addFundsId1).status).isEqualTo(ActionStatus.UndoInProgress);
                assertThat(state.actions.get(transferFundsId).status).isEqualTo(ActionStatus.Failed);
            }
        });

        scc.sagaStateTransitionVerifier.verifySingle((id, stateTransition) -> {
            assertThat(stateTransition).isInstanceOf(SagaStateTransition.TransitionList.class);
            SagaStateTransition.SagaActionStateChanged<?> c = ((SagaStateTransition.TransitionList<SpecificRecord>) stateTransition).actions.get(0);
            assertThat(c.actionId).isEqualTo(addFundsId1);
            assertThat(c.actionStatus).isEqualTo(ActionStatus.UndoInProgress);
        });

        // undo add funds successful
        scc.accountActionResponsePublisher.publish(saga.sagaId, ActionResponse.of(
                saga.sagaId, addFundsId1,
                commandIds.get(addFundsId1).undoAction,
                true,
                Result.success(Optional.empty())));

        verifyFinishUndo(scc, saga, sagaError2, 13);
    }

    @Test
    void testParallelSuccessful() {
        SagaCoordinatorContext scc = new SagaCoordinatorContext();

        Saga<SpecificRecord> saga = getParallelSaga();
        Map<ActionId, CommandIds> commandIds = getCommandIds(saga);
        scc.sagaRequestPublisher.publish(saga.sagaId, SagaRequest.of(SagaId.random(), saga));

        scc.accountActionRequestVerifier.verifyMultiple(2, (i, id, actionRequest) -> {
            assertThat(id).isEqualTo(saga.sagaId);
            assertThat(actionRequest.actionId).isIn(addFundsId1, addFundsId2);
            assertThat(actionRequest.actionCommand.actionType).isEqualTo(Constants.ACCOUNT_ACTION_TYPE);
        });

        scc.sagaStateTransitionVerifier.verifyMultiple(2, (i, id, stateTransition) -> {
        });

        scc.sagaStateVerifier.verifyMultiple(2, (i, id, state) -> {
        });

        scc.accountActionResponsePublisher.publish(saga.sagaId, ActionResponse.of(
                saga.sagaId,
                addFundsId1,
                commandIds.get(addFundsId1).action,
                false,
                Result.success(Optional.empty())));
        scc.accountActionResponsePublisher.publish(saga.sagaId, ActionResponse.of(
                        saga.sagaId, addFundsId2,
                commandIds.get(addFundsId2).action,
                false,
                Result.success(Optional.empty())));

        scc.sagaStateTransitionVerifier.verifyMultiple(3, (i, id, stateTransition) -> {
        });

        scc.sagaStateVerifier.verifyMultiple(3, (i, id, state) -> {
        });
        scc.sagaResponseVerifier.verifySingle((id, response) -> {
            assertThat(response.sagaId).isEqualTo(saga.sagaId);
            assertThat(response.result.isSuccess()).isTrue();
        });
    }

    @Test
    void testFailureInParallel() {
        SagaCoordinatorContext scc = new SagaCoordinatorContext();

        Saga<SpecificRecord> saga = getParallelSaga();
        Map<ActionId, CommandIds> commandIds = getCommandIds(saga);
        scc.sagaRequestPublisher.publish(saga.sagaId, SagaRequest.of(SagaId.random(), saga));

        scc.accountActionRequestVerifier.verifyMultiple(2, (i, id, actionRequest) -> {
            assertThat(id).isEqualTo(saga.sagaId);
            assertThat(actionRequest.actionId).isIn(addFundsId1, addFundsId2);
            assertThat(actionRequest.actionCommand.actionType).isEqualTo(Constants.ACCOUNT_ACTION_TYPE);
        });

        scc.sagaStateTransitionVerifier.verifyMultiple(2, (i, id, stateTransition) -> {
        });

        scc.sagaStateVerifier.verifyMultiple(2, (i, id, state) -> {
        });

        scc.accountActionResponsePublisher.publish(saga.sagaId, ActionResponse.of(
                saga.sagaId,
                addFundsId1,
                commandIds.get(addFundsId1).action,
                false,
                Result.success(Optional.empty())));
        SagaError sagaError = SagaError.of(SagaError.Reason.CommandError, "Oh noes");
        scc.accountActionResponsePublisher.publish(saga.sagaId, ActionResponse.of(
                saga.sagaId,
                addFundsId2,
                commandIds.get(addFundsId2).action,
                false,
                Result.failure(sagaError)));

        scc.sagaStateTransitionVerifier.verifyMultiple(4, (i, id, stateTransition) -> {
        });

        scc.sagaStateVerifier.verifyMultiple(4, (i, id, state) -> {
        });

        // undo non-failing action
        scc.accountActionRequestVerifier.verifySingle((id, actionRequest) -> {
            assertThat(id).isEqualTo(saga.sagaId);
            assertThat(actionRequest.sagaId).isEqualTo(saga.sagaId);
            assertThat(actionRequest.actionId).isEqualTo(addFundsId1);
            assertThat(actionRequest.actionCommand.actionType).isEqualTo(Constants.ACCOUNT_ACTION_TYPE);
            assertThat(actionRequest.actionCommand.commandId).isEqualTo(commandIds.get(addFundsId1).undoAction);
        });

        // undo successful
        scc.accountActionResponsePublisher.publish(saga.sagaId, ActionResponse.of(
                saga.sagaId,
                addFundsId1,
                commandIds.get(addFundsId1).undoAction,
                true,
                Result.success(Optional.empty())));

        scc.sagaResponseVerifier.verifySingle((id, response) -> {
            assertThat(response.sagaId).isEqualTo(saga.sagaId);
            assertThat(response.result.isSuccess()).isFalse();
            assertThat(response.result.failureReasons()).contains(NonEmptyList.of(sagaError));
        });
    }

    @Test
    void testWaitForParallelActionsToCompleteBeforeUndo() {
        SagaCoordinatorContext scc = new SagaCoordinatorContext();

        Saga<SpecificRecord> saga = getParallelSaga3Actions();
        Map<ActionId, CommandIds> commandIds = getCommandIds(saga);
        scc.sagaRequestPublisher.publish(saga.sagaId, SagaRequest.of(SagaId.random(), saga));

        scc.accountActionRequestVerifier.verifyMultiple(3, (i, id, actionRequest) -> {
            assertThat(id).isEqualTo(saga.sagaId);
            assertThat(actionRequest.actionId).isIn(addFundsId1, addFundsId2, transferFundsId);
        });

        scc.sagaStateTransitionVerifier.drainAll();
        scc.sagaStateVerifier.drainAll();

        scc.accountActionResponsePublisher.publish(saga.sagaId, ActionResponse.of(
                saga.sagaId,
                addFundsId1,
                commandIds.get(addFundsId1).undoAction,
                false,
                Result.success(Optional.empty())));

        scc.sagaStateVerifier.verifySingle((id, sagaState) -> {
            assertThat(sagaState.status).isEqualTo(SagaStatus.InProgress);
        });

        SagaError sagaError = SagaError.of(SagaError.Reason.CommandError, "Oh noes");
        scc.accountActionResponsePublisher.publish(saga.sagaId, ActionResponse.of(
                saga.sagaId,
                addFundsId2,
                commandIds.get(addFundsId2).undoAction,
                false,
                Result.failure(sagaError)));

        scc.sagaStateVerifier.verifyMultiple(2, (i, id, state) -> {
            if (i == 0) {
                assertThat(state.status).isEqualTo(SagaStatus.InProgress);
                assertThat(state.actions.get(addFundsId1).status).isEqualTo(ActionStatus.Completed);
                assertThat(state.actions.get(addFundsId2).status).isEqualTo(ActionStatus.Failed);
                assertThat(state.actions.get(transferFundsId).status).isEqualTo(ActionStatus.InProgress);
            } else if (i == 1) {
                assertThat(state.status).isEqualTo(SagaStatus.FailurePending);
                assertThat(state.actions.get(addFundsId1).status).isEqualTo(ActionStatus.Completed);
                assertThat(state.actions.get(addFundsId2).status).isEqualTo(ActionStatus.Failed);
                assertThat(state.actions.get(transferFundsId).status).isEqualTo(ActionStatus.InProgress);
            }
        });
        scc.accountActionRequestVerifier.verifyNoRecords();

        // final action completes
        scc.accountActionResponsePublisher.publish(saga.sagaId, ActionResponse.of(
                saga.sagaId,
                transferFundsId,
                commandIds.get(transferFundsId).action,
                false,
                Result.success(Optional.empty())));


        scc.sagaStateVerifier.verifyMultiple(3, (i, id, state) -> {
            if (i == 0) {
                assertThat(state.status).isEqualTo(SagaStatus.FailurePending);
                assertThat(state.actions.get(addFundsId1).status).isEqualTo(ActionStatus.Completed);
                assertThat(state.actions.get(addFundsId2).status).isEqualTo(ActionStatus.Failed);
                assertThat(state.actions.get(transferFundsId).status).isEqualTo(ActionStatus.Completed);
            } else if (i == 1) {
                assertThat(state.status).isEqualTo(SagaStatus.InFailure);
                assertThat(state.actions.get(addFundsId1).status).isEqualTo(ActionStatus.Completed);
                assertThat(state.actions.get(addFundsId2).status).isEqualTo(ActionStatus.Failed);
                assertThat(state.actions.get(transferFundsId).status).isEqualTo(ActionStatus.Completed);
            } else if (i == 2) {
                assertThat(state.status).isEqualTo(SagaStatus.InFailure);
                assertThat(state.actions.get(addFundsId1).status).isEqualTo(ActionStatus.UndoInProgress);
                assertThat(state.actions.get(addFundsId2).status).isEqualTo(ActionStatus.Failed);
                assertThat(state.actions.get(transferFundsId).status).isEqualTo(ActionStatus.UndoInProgress);
            }
        });

        // undo non-failing actions
        scc.accountActionRequestVerifier.verifyMultiple(2, (i, id, actionRequest) -> {
            assertThat(id).isEqualTo(saga.sagaId);
            assertThat(actionRequest.sagaId).isEqualTo(saga.sagaId);
            assertThat(actionRequest.actionCommand.actionType).isEqualTo(Constants.ACCOUNT_ACTION_TYPE);
            assertThat(actionRequest.actionCommand.commandId).isIn(commandIds.get(addFundsId1).undoAction, commandIds.get(transferFundsId).undoAction);
        });

        // undo successful
        scc.accountActionResponsePublisher.publish(saga.sagaId, ActionResponse.of(
                saga.sagaId,
                addFundsId1,
                commandIds.get(addFundsId1).undoAction,
                true,
                Result.success(Optional.empty())));
        scc.accountActionResponsePublisher.publish(saga.sagaId, ActionResponse.of(
                saga.sagaId,
                transferFundsId,
                commandIds.get(transferFundsId).undoAction,
                true,
                Result.success(Optional.empty())));

        scc.sagaResponseVerifier.verifySingle((id, response) -> {
            assertThat(response.sagaId).isEqualTo(saga.sagaId);
            assertThat(response.result.isSuccess()).isFalse();
            assertThat(response.result.failureReasons()).contains(NonEmptyList.of(sagaError));
        });
    }

    @Test
    void testShortCircuitRetryOnParallelActionsBeforeUndo() {
        // TODO: verify that if a retry is in progress, and an action fails, that retry is not invoked
        SagaCoordinatorContext scc = new SagaCoordinatorContext(1);

        Saga<SpecificRecord> saga = getParallelSaga3Actions();
        Map<ActionId, CommandIds> commandIds = getCommandIds(saga);
        scc.sagaRequestPublisher.publish(saga.sagaId, SagaRequest.of(SagaId.random(), saga));

        scc.accountActionRequestVerifier.verifyMultiple(3, (i, id, actionRequest) -> {
            assertThat(id).isEqualTo(saga.sagaId);
            assertThat(actionRequest.actionId).isIn(addFundsId1, addFundsId2, transferFundsId);
        });

        scc.sagaStateTransitionVerifier.drainAll();
        scc.sagaStateVerifier.drainAll();

        scc.accountActionResponsePublisher.publish(saga.sagaId, ActionResponse.of(
                saga.sagaId,
                addFundsId1,
                commandIds.get(addFundsId1).undoAction,
                false,
                Result.success(Optional.empty())));

        scc.sagaStateVerifier.verifySingle();

        SagaError sagaError1 = SagaError.of(SagaError.Reason.CommandError, "Oh noes 1");
        scc.accountActionResponsePublisher.publish(saga.sagaId, ActionResponse.of(
                saga.sagaId,
                addFundsId1,
                commandIds.get(addFundsId1).undoAction,
                false,
                Result.failure(sagaError1)));

        delayMillis(300);
        SagaStateTransition<SpecificRecord> retryTransition1 = scc.testRetryPublisher.getTransition(Constants.ACCOUNT_ACTION_TYPE, saga.sagaId);
        assertThat(retryTransition1).isInstanceOf(SagaStateTransition.SagaActionStateChanged.class);

        SagaError sagaError2 = SagaError.of(SagaError.Reason.CommandError, "Oh noes 2");
        scc.accountActionResponsePublisher.publish(saga.sagaId, ActionResponse.of(
                saga.sagaId,
                addFundsId2,
                commandIds.get(addFundsId2).undoAction,
                false,
                Result.failure(sagaError2)));

        delayMillis(300);
        SagaStateTransition<SpecificRecord> retryTransition2 = scc.testRetryPublisher.getTransition(Constants.ACCOUNT_ACTION_TYPE, saga.sagaId);
        assertThat(retryTransition1).isInstanceOf(SagaStateTransition.SagaActionStateChanged.class);

        scc.sagaStateVerifier.verifyMultiple(2, (i, id, state) -> {
            if (i == 0) {
                assertThat(state.status).isEqualTo(SagaStatus.InProgress);
                assertThat(state.actions.get(addFundsId1).status).isEqualTo(ActionStatus.RetryAwaiting);
                assertThat(state.actions.get(addFundsId2).status).isEqualTo(ActionStatus.InProgress);
                assertThat(state.actions.get(transferFundsId).status).isEqualTo(ActionStatus.InProgress);
            } else if (i == 1) {
                assertThat(state.status).isEqualTo(SagaStatus.InProgress);
                assertThat(state.actions.get(addFundsId1).status).isEqualTo(ActionStatus.RetryAwaiting);
                assertThat(state.actions.get(addFundsId2).status).isEqualTo(ActionStatus.RetryAwaiting);
                assertThat(state.actions.get(transferFundsId).status).isEqualTo(ActionStatus.InProgress);
            }
        });
        scc.accountActionRequestVerifier.verifyNoRecords();

        SagaStateTransition.SagaActionStateChanged<?> retryStateChange1 = ((SagaStateTransition.SagaActionStateChanged) retryTransition1);
        assertThat(retryStateChange1).isEqualTo(SagaStateTransition.SagaActionStateChanged.of(
                saga.sagaId,
                addFundsId1,
                ActionStatus.RetryCompleted,
                Collections.emptyList(), Optional.empty(),
                false));

        SagaStateTransition.SagaActionStateChanged<?> retryStateChange2 = ((SagaStateTransition.SagaActionStateChanged) retryTransition2);
        assertThat(retryStateChange2).isEqualTo(SagaStateTransition.SagaActionStateChanged.of(
                saga.sagaId,
                addFundsId2,
                ActionStatus.RetryCompleted,
                Collections.emptyList(), Optional.empty(),
                false));

        // complete the async setting of action back to pending, so that it can be rerun
        scc.sagaStateTransitionPublisher.publish(saga.sagaId, retryTransition1);
        scc.accountActionRequestVerifier.verifySingle((id, request) -> {
            assertThat(request.actionId).isEqualTo(addFundsId1);
        });

        scc.sagaStateVerifier.verifyMultiple(2, (i, id, state) -> {
            if (i == 0) {
                assertThat(state.status).isEqualTo(SagaStatus.InProgress);
                assertThat(state.actions.get(addFundsId1).status).isEqualTo(ActionStatus.Pending);
                assertThat(state.actions.get(addFundsId2).status).isEqualTo(ActionStatus.RetryAwaiting);
                assertThat(state.actions.get(transferFundsId).status).isEqualTo(ActionStatus.InProgress);
            } else if (i == 1) {
                assertThat(state.status).isEqualTo(SagaStatus.InProgress);
                assertThat(state.actions.get(addFundsId1).status).isEqualTo(ActionStatus.InProgress);
                assertThat(state.actions.get(addFundsId2).status).isEqualTo(ActionStatus.RetryAwaiting);
                assertThat(state.actions.get(transferFundsId).status).isEqualTo(ActionStatus.InProgress);
            }
        });

        SagaError sagaError1Again = SagaError.of(SagaError.Reason.CommandError, "Oh noes 1 again");
        scc.accountActionResponsePublisher.publish(saga.sagaId, ActionResponse.of(
                saga.sagaId,
                addFundsId1,
                commandIds.get(addFundsId1).undoAction,
                false,
                Result.failure(sagaError1Again)));
        scc.accountActionRequestVerifier.verifyNoRecords();

        scc.sagaStateVerifier.verifyMultiple(2, (i, id, state) -> {
            if (i == 0) {
                assertThat(state.status).isEqualTo(SagaStatus.InProgress);
                assertThat(state.actions.get(addFundsId1).status).isEqualTo(ActionStatus.Failed);
                assertThat(state.actions.get(addFundsId2).status).isEqualTo(ActionStatus.RetryAwaiting);
                assertThat(state.actions.get(transferFundsId).status).isEqualTo(ActionStatus.InProgress);
            } else if (i == 1) {
                assertThat(state.status).isEqualTo(SagaStatus.FailurePending);
                assertThat(state.actions.get(addFundsId1).status).isEqualTo(ActionStatus.Failed);
                assertThat(state.actions.get(addFundsId2).status).isEqualTo(ActionStatus.Failed);
                assertThat(state.actions.get(transferFundsId).status).isEqualTo(ActionStatus.InProgress);
            }
        });

        // complete the async setting of action back to pending, so that it can be rerun
        scc.sagaStateTransitionPublisher.publish(saga.sagaId, retryTransition2);
        scc.accountActionRequestVerifier.verifyNoRecords();

        scc.sagaStateVerifier.drainAll();
        // final action completes
        scc.accountActionResponsePublisher.publish(saga.sagaId, ActionResponse.of(
                saga.sagaId,
                transferFundsId,
                commandIds.get(transferFundsId).action,
                false,
                Result.success(Optional.empty())));


        scc.sagaStateVerifier.verifyMultiple(3, (i, id, state) -> {
            if (i == 0) {
                assertThat(state.status).isEqualTo(SagaStatus.FailurePending);
                assertThat(state.actions.get(addFundsId1).status).isEqualTo(ActionStatus.Failed);
                assertThat(state.actions.get(addFundsId2).status).isEqualTo(ActionStatus.Failed);
                assertThat(state.actions.get(transferFundsId).status).isEqualTo(ActionStatus.Completed);
            } else if (i == 1) {
                assertThat(state.status).isEqualTo(SagaStatus.InFailure);
                assertThat(state.actions.get(addFundsId1).status).isEqualTo(ActionStatus.Failed);
                assertThat(state.actions.get(addFundsId2).status).isEqualTo(ActionStatus.Failed);
                assertThat(state.actions.get(transferFundsId).status).isEqualTo(ActionStatus.Completed);
            } else if (i == 2) {
                assertThat(state.status).isEqualTo(SagaStatus.InFailure);
                assertThat(state.actions.get(addFundsId1).status).isEqualTo(ActionStatus.Failed);
                assertThat(state.actions.get(addFundsId2).status).isEqualTo(ActionStatus.Failed);
                assertThat(state.actions.get(transferFundsId).status).isEqualTo(ActionStatus.UndoInProgress);
            }
        });

        // undo non-failing actions
        scc.accountActionRequestVerifier.verifySingle((id, actionRequest) -> {
            assertThat(id).isEqualTo(saga.sagaId);
            assertThat(actionRequest.sagaId).isEqualTo(saga.sagaId);
            assertThat(actionRequest.actionCommand.actionType).isEqualTo(Constants.ACCOUNT_ACTION_TYPE);
            assertThat(actionRequest.actionId).isEqualTo(transferFundsId);
        });

        // undo successful
        scc.accountActionResponsePublisher.publish(saga.sagaId, ActionResponse.of(
                saga.sagaId,
                transferFundsId,
                commandIds.get(transferFundsId).undoAction,
                true,
                Result.success(Optional.empty())));

        scc.sagaResponseVerifier.verifySingle((id, response) -> {
            assertThat(response.sagaId).isEqualTo(saga.sagaId);
            assertThat(response.result.isSuccess()).isFalse();
            assertThat(response.result.failureReasons().isPresent()).isTrue();
            assertThat(response.result.failureReasons().get().toList()).containsExactlyInAnyOrder(sagaError1Again, sagaError2);
        });
    }

    @Test
    void testMultiActionTypeSaga() {
        SagaCoordinatorContext scc = new SagaCoordinatorContext();

        SagaId sagaRequestId = SagaId.random();
        Saga<SpecificRecord> saga = getMultiActionTypeSaga();
        Map<ActionId, CommandIds> commandIds = getCommandIds(saga);

        scc.sagaRequestPublisher.publish(saga.sagaId, SagaRequest.of(sagaRequestId, saga));

        scc.userActionRequestVerifier.verifySingle((id, actionRequest) -> {
            assertThat(id).isEqualTo(saga.sagaId);
            assertThat(actionRequest.actionCommand.actionType).isEqualTo(Constants.USER_ACTION_TYPE);
            assertThat(actionRequest.actionId).isEqualTo(createUserId);
        });
        scc.accountActionRequestVerifier.verifyNoRecords();

        // create user successful
        scc.userActionResponsePublisher.publish(saga.sagaId, ActionResponse.of(
                saga.sagaId,
                createUserId,
                commandIds.get(createUserId).action,
                false,
                Result.success(Optional.empty())));

        scc.accountActionRequestVerifier.verifySingle((id, actionRequest) -> {
            assertThat(id).isEqualTo(saga.sagaId);
            assertThat(actionRequest.sagaId).isEqualTo(saga.sagaId);
            assertThat(actionRequest.actionCommand.actionType).isEqualTo(Constants.ACCOUNT_ACTION_TYPE);
            assertThat(actionRequest.actionId).isEqualTo(createAccountId);
        });
        scc.userActionRequestVerifier.verifyNoRecords();

        // add account successful
        scc.accountActionResponsePublisher.publish(saga.sagaId, ActionResponse.of(
                saga.sagaId,
                createAccountId,
                commandIds.get(createAccountId).action,
                false,
                Result.success(Optional.empty())));
        scc.userActionRequestVerifier.verifyNoRecords();
        scc.accountActionRequestVerifier.verifyNoRecords();

        scc.sagaResponseVerifier.verifySingle((id, response) -> {
            assertThat(response.sagaId).isEqualTo(saga.sagaId);
            assertThat(response.result.isSuccess()).isTrue();
        });
    }

    @Test
    void testInvalidActionSaga() {
        SagaCoordinatorContext scc = new SagaCoordinatorContext();

        SagaId sagaRequestId = SagaId.random();
        Saga<SpecificRecord> saga = getInvalidActionSaga();

        scc.sagaRequestPublisher.publish(saga.sagaId, SagaRequest.of(sagaRequestId, saga));

        scc.accountActionRequestVerifier.verifyNoRecords();
        scc.sagaStateTransitionVerifier.verifyNoRecords();
        scc.sagaStateVerifier.verifyNoRecords();

        scc.sagaResponseVerifier.verifySingle((id, response) -> {
            assertThat(response.sagaId).isEqualTo(saga.sagaId);
            assertThat(response.result.isFailure()).isTrue();
            assertThat(response.result.failureReasons().isPresent()).isTrue();
            response.result.failureReasons().ifPresent(reasons -> {
                reasons.toList().forEach(r -> {
                    assertThat(r.error().getReason()).isEqualTo(SagaError.Reason.InvalidSaga);
                    assertThat(r.error().getMessage()).startsWith("Unknown action type");
                });
            });
        });
    }
}