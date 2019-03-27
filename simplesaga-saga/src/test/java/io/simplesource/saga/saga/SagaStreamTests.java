package io.simplesource.saga.saga;

import io.simplesource.api.CommandId;
import io.simplesource.data.NonEmptyList;
import io.simplesource.data.Result;
import io.simplesource.kafka.spec.WindowSpec;
import io.simplesource.saga.client.dsl.SagaDsl;
import io.simplesource.saga.model.action.ActionId;
import io.simplesource.saga.model.action.ActionStatus;
import io.simplesource.saga.model.action.SagaAction;
import io.simplesource.saga.model.messages.*;
import io.simplesource.saga.model.saga.Saga;
import io.simplesource.saga.model.saga.SagaError;
import io.simplesource.saga.model.saga.SagaId;
import io.simplesource.saga.model.saga.SagaStatus;
import io.simplesource.saga.model.serdes.ActionSerdes;
import io.simplesource.saga.model.serdes.SagaSerdes;
import io.simplesource.saga.model.specs.ActionProcessorSpec;
import io.simplesource.saga.model.specs.SagaSpec;
import io.simplesource.saga.saga.avro.generated.test.AddFunds;
import io.simplesource.saga.saga.avro.generated.test.CreateAccount;
import io.simplesource.saga.saga.avro.generated.test.TransferFunds;
import io.simplesource.saga.serialization.avro.AvroSerdes;
import io.simplesource.saga.shared.topics.TopicNamer;
import io.simplesource.saga.shared.topics.TopicTypes;
import io.simplesource.saga.testutils.*;
import lombok.Value;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.streams.Topology;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.simplesource.saga.client.dsl.SagaDsl.inParallel;
import static org.assertj.core.api.Assertions.assertThat;

class SagaStreamTests {

    private static String SCHEMA_URL = "http://localhost:8081/";

    @Value
    private static class SagaCoordinatorContext {
        final TestContext testContext;
        // serdes
        final SagaSerdes<SpecificRecord> sagaSerdes = AvroSerdes.Specific.sagaSerdes(SCHEMA_URL, true);
        final ActionSerdes<SpecificRecord> actionSerdes = AvroSerdes.Specific.actionSerdes(SCHEMA_URL, true);

        // publishers
        final RecordPublisher<SagaId, SagaRequest<SpecificRecord>> sagaRequestPublisher;
        final RecordPublisher<SagaId, ActionResponse> actionResponsePublisher;

        // verifiers
        final RecordVerifier<SagaId, ActionRequest<SpecificRecord>> actionRequestVerifier;
        final RecordVerifier<SagaId, SagaStateTransition> sagaStateTransitionVerifier;
        final RecordVerifier<SagaId, Saga<SpecificRecord>> sagaStateVerifier;
        final RecordVerifier<SagaId, SagaResponse> sagaResponseVerifier;

        SagaCoordinatorContext() {
            TopicNamer sagaTopicNamer = TopicNamer.forPrefix(Constants.SAGA_TOPIC_PREFIX, TopicTypes.SagaTopic.SAGA_BASE_NAME);
            TopicNamer actionTopicNamer = TopicNamer.forPrefix(Constants.ACTION_TOPIC_PREFIX, Constants.SAGA_ACTION_TYPE);

            SagaApp<SpecificRecord> sagaApp = new SagaApp<>(
                    new SagaSpec<>(sagaSerdes, new WindowSpec(60)),
                    ActionProcessorSpec.of(actionSerdes),
                    topicBuilder -> topicBuilder.withTopicPrefix(Constants.SAGA_TOPIC_PREFIX));

            sagaApp.addActionProcessor(
                    Constants.SAGA_ACTION_TYPE,
                    topicBuilder -> topicBuilder.withTopicPrefix(Constants.ACTION_TOPIC_PREFIX));

            Topology topology = sagaApp.buildTopology();
            testContext = TestContextBuilder.of(topology).build();

            sagaRequestPublisher = testContext.publisher(
                    sagaTopicNamer.apply(TopicTypes.SagaTopic.SAGA_REQUEST),
                    sagaSerdes.sagaId(),
                    sagaSerdes.request());
            actionResponsePublisher = testContext.publisher(
                    actionTopicNamer.apply(TopicTypes.ActionTopic.ACTION_RESPONSE),
                    actionSerdes.sagaId(),
                    actionSerdes.response());

            actionRequestVerifier = testContext.verifier(
                    actionTopicNamer.apply(TopicTypes.ActionTopic.ACTION_REQUEST),
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

    private ActionId createAccountId = ActionId.random();
    private ActionId addFundsId1 = ActionId.random();
    private ActionId addFundsId2 = ActionId.random();
    private ActionId transferFundsId = ActionId.random();

    Saga<SpecificRecord> getBasicSaga() {
        SagaDsl.SagaBuilder<SpecificRecord> builder = SagaDsl.SagaBuilder.create();

        SagaDsl.SubSaga<SpecificRecord> createAccount = builder.addAction(
                createAccountId,
                "createAccount",
                new CreateAccount("id1", "User 1"));

        SagaDsl.SubSaga<SpecificRecord> addFunds = builder.addAction(
                addFundsId1,
                "addFunds",
                new AddFunds("id1", 1000.0),
                // this will never undo since it's in the last sub-saga
                new AddFunds("id1", -1000.0));

        createAccount.andThen(addFunds);

        Result<SagaError, Saga<SpecificRecord>> sagaBuildResult = builder.build();
        assertThat(sagaBuildResult.isSuccess()).isEqualTo(true);

        return sagaBuildResult.getOrElse(null);
    }

    Saga<SpecificRecord> getSagaWithUndo() {
        SagaDsl.SagaBuilder<SpecificRecord> builder = SagaDsl.SagaBuilder.create();

        SagaDsl.SubSaga<SpecificRecord> addFunds = builder.addAction(
                addFundsId1,
                "addFunds",
                new AddFunds("id1", 1000.0),
                new AddFunds("id1", -1000.0));

        SagaDsl.SubSaga<SpecificRecord> transferFunds = builder.addAction(
                transferFundsId,
                "transferFunds",
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
        for (SagaAction<SpecificRecord>a: saga.actions.values()) {
            commandIdsHashMap.put(a.actionId, new CommandIds(a.command.commandId, a.undoCommand.map(c -> c.commandId).orElse(null)));
        }
        return commandIdsHashMap;
    }

    Saga<SpecificRecord> getParallelSaga() {
        SagaDsl.SagaBuilder<SpecificRecord> builder = SagaDsl.SagaBuilder.create();

        SagaDsl.SubSaga<SpecificRecord> addFunds1 = builder.addAction(
                addFundsId1,
                "addFunds",
                new AddFunds("id1", 1000.0),
                new AddFunds("id1", -1000.0));
        SagaDsl.SubSaga<SpecificRecord> addFunds2 = builder.addAction(
                addFundsId2,
                "addFunds",
                new AddFunds("id2", 1000.0),
                new AddFunds("id2", -1000.0));

        inParallel(addFunds1, addFunds2);

        Result<SagaError, Saga<SpecificRecord>> sagaBuildResult = builder.build();
        assertThat(sagaBuildResult.isSuccess()).isEqualTo(true);

        return sagaBuildResult.getOrElse(null);
    }

    Saga<SpecificRecord> getParallelSaga3Actions() {
        SagaDsl.SagaBuilder<SpecificRecord> builder = SagaDsl.SagaBuilder.create();

        SagaDsl.SubSaga<SpecificRecord> addFunds1 = builder.addAction(
                addFundsId1,
                "addFunds",
                new AddFunds("id1", 1000.0),
                new AddFunds("id1", -1000.0));
        SagaDsl.SubSaga<SpecificRecord> addFunds2 = builder.addAction(
                addFundsId2,
                "addFunds",
                new AddFunds("id2", 1000.0),
                new AddFunds("id2", -1000.0));
        SagaDsl.SubSaga<SpecificRecord> transferFunds = builder.addAction(
                transferFundsId,
                "transferFunds",
                new TransferFunds("id3", "id4", 10.0),
                new TransferFunds("id4", "id3", 10.0));

        inParallel(addFunds1, addFunds2, transferFunds);

        Result<SagaError, Saga<SpecificRecord>> sagaBuildResult = builder.build();
        assertThat(sagaBuildResult.isSuccess()).isEqualTo(true);

        return sagaBuildResult.getOrElse(null);
    }

    @Test
    void testSuccessfulSaga() {
        SagaCoordinatorContext scc = new SagaCoordinatorContext();

        SagaId sagaRequestId = SagaId.random();
        Saga<SpecificRecord> saga = getBasicSaga();
        Map<ActionId, CommandIds> commandIds = getCommandIds(saga);

        scc.sagaRequestPublisher.publish(saga.sagaId, new SagaRequest<>(sagaRequestId, saga));

        scc.actionRequestVerifier.verifySingle((id, actionRequest) -> {
            assertThat(id).isEqualTo(saga.sagaId);
            assertThat(actionRequest.actionType).isEqualTo("createAccount");
            assertThat(actionRequest.actionId).isEqualTo(createAccountId);
            assertThat(actionRequest.actionCommand.commandId).isEqualTo(commandIds.get(createAccountId).action);
        });
        scc.actionRequestVerifier.verifyNoRecords();

        scc.sagaStateTransitionVerifier.verifyMultiple(2, (i, id, stateTransition) -> {
            if (i == 0) {
                assertThat(stateTransition).isInstanceOf(SagaStateTransition.SetInitialState.class);
                Saga s = ((SagaStateTransition.SetInitialState) stateTransition).sagaState;
                assertThat(s.status).isEqualTo(SagaStatus.NotStarted);
                assertThat(s.sequence.getSeq()).isEqualTo(0);
                assertThat(s.actions).containsKeys(createAccountId, addFundsId1);
            } else if (i == 1) {
                assertThat(stateTransition).isInstanceOf(SagaStateTransition.TransitionList.class);
                SagaStateTransition.TransitionList transitionList = ((SagaStateTransition.TransitionList) stateTransition);
                assertThat(transitionList.actions.size()).isEqualTo(1);
                assertThat(transitionList.actions.get(0).actionStatus).isEqualTo(ActionStatus.InProgress);
            }
        });
        scc.sagaStateTransitionVerifier.verifyNoRecords();

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
        scc.sagaStateVerifier.verifyNoRecords();

        // create account successful
        scc.actionResponsePublisher.publish(saga.sagaId, new ActionResponse(saga.sagaId, createAccountId, commandIds.get(createAccountId).action, Result.success(true)));

        scc.sagaStateTransitionVerifier.verifyMultiple(2, (i, id, stateTransition) -> {
            if (i == 0) {
                assertThat(stateTransition).isInstanceOf(SagaStateTransition.SagaActionStatusChanged.class);
                SagaStateTransition.SagaActionStatusChanged c = ((SagaStateTransition.SagaActionStatusChanged) stateTransition);
                assertThat(c.actionId).isEqualTo(createAccountId);
                assertThat(c.actionStatus).isEqualTo(ActionStatus.Completed);
            } else if (i == 1) {
                assertThat(stateTransition).isInstanceOf(SagaStateTransition.TransitionList.class);
                SagaStateTransition.TransitionList transitionList = ((SagaStateTransition.TransitionList) stateTransition);
                assertThat(transitionList.actions.size()).isEqualTo(1);
                assertThat(transitionList.actions.get(0).actionStatus).isEqualTo(ActionStatus.InProgress);
            }
        });
        scc.sagaStateTransitionVerifier.verifyNoRecords();

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
        scc.sagaStateVerifier.verifyNoRecords();

        scc.actionRequestVerifier.verifySingle((id, actionRequest) -> {
            assertThat(id).isEqualTo(saga.sagaId);
            assertThat(actionRequest.sagaId).isEqualTo(saga.sagaId);
            assertThat(actionRequest.actionType).isEqualTo("addFunds");
            assertThat(actionRequest.actionId).isEqualTo(addFundsId1);
            assertThat(actionRequest.actionCommand.commandId).isEqualTo(commandIds.get(addFundsId1).action);
        });
        scc.actionRequestVerifier.verifyNoRecords();

        // add funds successful
        scc.actionResponsePublisher.publish(saga.sagaId, new ActionResponse(saga.sagaId, addFundsId1, commandIds.get(addFundsId1).action, Result.success(true)));

        scc.sagaStateTransitionVerifier.verifyMultiple(2, (i, id, stateTransition) -> {
            if (i == 0) {
                assertThat(stateTransition).isInstanceOf(SagaStateTransition.SagaActionStatusChanged.class);
                SagaStateTransition.SagaActionStatusChanged c = ((SagaStateTransition.SagaActionStatusChanged) stateTransition);
                assertThat(c.actionId).isEqualTo(addFundsId1);
                assertThat(c.actionStatus).isEqualTo(ActionStatus.Completed);
            } else if (i == 1) {
                assertThat(stateTransition).isInstanceOf(SagaStateTransition.SagaStatusChanged.class);
                SagaStateTransition.SagaStatusChanged c = ((SagaStateTransition.SagaStatusChanged) stateTransition);
                assertThat(c.sagaId).isEqualTo(saga.sagaId);
                assertThat(c.sagaStatus).isEqualTo(SagaStatus.Completed);
            }
        });
        scc.sagaStateTransitionVerifier.verifyNoRecords();

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
                assertThat(state.actions.get(addFundsId1).status).isEqualTo(ActionStatus.Completed);
            }
        });
        scc.sagaStateVerifier.verifyNoRecords();
        scc.actionRequestVerifier.verifyNoRecords();

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
        scc.sagaRequestPublisher.publish(saga.sagaId, new SagaRequest<>(sagaRequestId, saga));

        // already verified in above test
        scc.actionRequestVerifier.drainAll();
        scc.sagaStateTransitionVerifier.drainAll();
        scc.sagaStateVerifier.drainAll();

        // create account failed
        SagaError sagaError = SagaError.of(SagaError.Reason.CommandError, "Oh noes");
        scc.actionResponsePublisher.publish(saga.sagaId, new ActionResponse(saga.sagaId, createAccountId, commandIds.get(createAccountId).action, Result.failure(sagaError)));

        scc.sagaStateTransitionVerifier.verifyMultiple(2, (i, id, stateTransition) -> {
            if (i == 0) {
                assertThat(stateTransition).isInstanceOf(SagaStateTransition.SagaActionStatusChanged.class);
                SagaStateTransition.SagaActionStatusChanged c = ((SagaStateTransition.SagaActionStatusChanged) stateTransition);
                assertThat(c.actionId).isEqualTo(createAccountId);
                assertThat(c.actionStatus).isEqualTo(ActionStatus.Failed);
            } else if (i == 1) {
                assertThat(stateTransition).isInstanceOf(SagaStateTransition.SagaStatusChanged.class);
                SagaStateTransition.SagaStatusChanged c = ((SagaStateTransition.SagaStatusChanged) stateTransition);
                assertThat(c.sagaStatus).isEqualTo(SagaStatus.Failed);
            }
        });
        scc.sagaStateTransitionVerifier.verifyNoRecords();

        scc.sagaStateVerifier.verifyMultiple(2, (i, id, state) -> {
            if (i == 0) {
                assertThat(state.sequence.getSeq()).isEqualTo(2);
                assertThat(state.status).isEqualTo(SagaStatus.InProgress);
                assertThat(state.actions.get(createAccountId).status).isEqualTo(ActionStatus.Failed);
                assertThat(state.actions.get(addFundsId1).status).isEqualTo(ActionStatus.Pending);
            } else if (i == 1) {
                assertThat(state.sequence.getSeq()).isEqualTo(3);
                assertThat(state.status).isEqualTo(SagaStatus.Failed);
                assertThat(state.actions.get(createAccountId).status).isEqualTo(ActionStatus.Failed);
                assertThat(state.actions.get(addFundsId1).status).isEqualTo(ActionStatus.Pending);
            }
        });
        scc.sagaStateVerifier.verifyNoRecords();

        scc.actionRequestVerifier.verifyNoRecords();

        scc.sagaResponseVerifier.verifySingle((id, response) -> {
            assertThat(response.sagaId).isEqualTo(saga.sagaId);
            assertThat(response.result.isSuccess()).isFalse();
            assertThat(response.result.failureReasons()).contains(NonEmptyList.of(sagaError));
        });
    }

    @Test
    void testBypassUndoOnFailureIfNotDefined() {
        SagaCoordinatorContext scc = new SagaCoordinatorContext();

        SagaId sagaRequestId = SagaId.random();
        Saga<SpecificRecord> saga = getBasicSaga();
        Map<ActionId, CommandIds> commandIds = getCommandIds(saga);
        scc.sagaRequestPublisher.publish(saga.sagaId, new SagaRequest<>(sagaRequestId, saga));

        // create account successful
        scc.actionResponsePublisher.publish(saga.sagaId, new ActionResponse(saga.sagaId, createAccountId, commandIds.get(createAccountId).action, Result.success(true)));

        scc.actionRequestVerifier.drainAll();
        scc.sagaStateTransitionVerifier.drainAll();
        scc.sagaStateVerifier.drainAll();

        // add funds failed
        SagaError sagaError = SagaError.of(SagaError.Reason.CommandError, "Oh noes");
        scc.actionResponsePublisher.publish(saga.sagaId, new ActionResponse(saga.sagaId, addFundsId1, commandIds.get(addFundsId1).action, Result.failure(sagaError)));

        scc.sagaStateTransitionVerifier.verifyMultiple(4, (i, id, stateTransition) -> {
            if (i == 0) {
                assertThat(stateTransition).isInstanceOf(SagaStateTransition.SagaActionStatusChanged.class);
                SagaStateTransition.SagaActionStatusChanged c = ((SagaStateTransition.SagaActionStatusChanged) stateTransition);
                assertThat(c.actionId).isEqualTo(addFundsId1);
                assertThat(c.actionStatus).isEqualTo(ActionStatus.Failed);
            } else if (i == 1) {
                assertThat(stateTransition).isInstanceOf(SagaStateTransition.SagaStatusChanged.class);
                SagaStateTransition.SagaStatusChanged c = ((SagaStateTransition.SagaStatusChanged) stateTransition);
                assertThat(c.sagaStatus).isEqualTo(SagaStatus.InFailure);
            } else if (i == 2) {
                assertThat(stateTransition).isInstanceOf(SagaStateTransition.TransitionList.class);
                SagaStateTransition.TransitionList transitionList = ((SagaStateTransition.TransitionList) stateTransition);
                assertThat(transitionList.actions.size()).isEqualTo(1);
                assertThat(transitionList.actions.get(0).actionStatus).isEqualTo(ActionStatus.UndoBypassed);
            } else if (i == 3) {
                assertThat(stateTransition).isInstanceOf(SagaStateTransition.SagaStatusChanged.class);
                SagaStateTransition.SagaStatusChanged c = ((SagaStateTransition.SagaStatusChanged) stateTransition);
                assertThat(c.sagaStatus).isEqualTo(SagaStatus.Failed);
            }
        });
        scc.sagaStateTransitionVerifier.verifyNoRecords();

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
        scc.sagaStateVerifier.verifyNoRecords();

        scc.actionRequestVerifier.verifyNoRecords();

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
        scc.sagaRequestPublisher.publish(saga.sagaId, new SagaRequest<>(sagaRequestId, saga));

        // add funds successful
        scc.actionResponsePublisher.publish(saga.sagaId, new ActionResponse(saga.sagaId, addFundsId1, commandIds.get(addFundsId1).action, Result.success(true)));

        scc.actionRequestVerifier.drainAll();
        scc.sagaStateTransitionVerifier.drainAll();
        scc.sagaStateVerifier.drainAll();

        // transfer funds failed
        SagaError sagaError = SagaError.of(SagaError.Reason.CommandError, "Oh noes");
        scc.actionResponsePublisher.publish(saga.sagaId, new ActionResponse(saga.sagaId, transferFundsId, commandIds.get(transferFundsId).action, Result.failure(sagaError)));

        scc.sagaStateTransitionVerifier.verifyMultiple(3, (i, id, stateTransition) -> {
            if (i == 0) {
                assertThat(stateTransition).isInstanceOf(SagaStateTransition.SagaActionStatusChanged.class);
                SagaStateTransition.SagaActionStatusChanged c = ((SagaStateTransition.SagaActionStatusChanged) stateTransition);
                assertThat(c.actionId).isEqualTo(transferFundsId);
                assertThat(c.actionStatus).isEqualTo(ActionStatus.Failed);
            } else if (i == 1) {
                assertThat(stateTransition).isInstanceOf(SagaStateTransition.SagaStatusChanged.class);
                SagaStateTransition.SagaStatusChanged c = ((SagaStateTransition.SagaStatusChanged) stateTransition);
                assertThat(c.sagaStatus).isEqualTo(SagaStatus.InFailure);
            } else if (i == 2) {
                assertThat(stateTransition).isInstanceOf(SagaStateTransition.TransitionList.class);
                SagaStateTransition.TransitionList transitionList = ((SagaStateTransition.TransitionList) stateTransition);
                assertThat(transitionList.actions.size()).isEqualTo(1);
                assertThat(transitionList.actions.get(0).actionStatus).isEqualTo(ActionStatus.InUndo);
            }
        });
        scc.sagaStateTransitionVerifier.verifyNoRecords();

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
                assertThat(state.actions.get(addFundsId1).status).isEqualTo(ActionStatus.InUndo);
                assertThat(state.actions.get(transferFundsId).status).isEqualTo(ActionStatus.Failed);
            }
        });
        scc.sagaStateVerifier.verifyNoRecords();

        scc.actionRequestVerifier.verifySingle((id, actionRequest) -> {
            assertThat(id).isEqualTo(saga.sagaId);
            assertThat(actionRequest.sagaId).isEqualTo(saga.sagaId);
            assertThat(actionRequest.actionType).isEqualTo("addFunds");
            assertThat(actionRequest.actionId).isEqualTo(addFundsId1);
            assertThat(actionRequest.actionCommand.commandId).isEqualTo(commandIds.get(addFundsId1).undoAction);
        });
        scc.actionRequestVerifier.verifyNoRecords();

        // undo add funds successful
        scc.actionResponsePublisher.publish(saga.sagaId, new ActionResponse(saga.sagaId, addFundsId1, commandIds.get(addFundsId1).undoAction, Result.success(true)));

        scc.sagaStateTransitionVerifier.verifyMultiple(2, (i, id, stateTransition) -> {
            if (i == 0) {
                assertThat(stateTransition).isInstanceOf(SagaStateTransition.SagaActionStatusChanged.class);
                SagaStateTransition.SagaActionStatusChanged c = ((SagaStateTransition.SagaActionStatusChanged) stateTransition);
                assertThat(c.actionId).isEqualTo(addFundsId1);
                assertThat(c.actionStatus).isEqualTo(ActionStatus.Completed);
            } else if (i == 1) {
                assertThat(stateTransition).isInstanceOf(SagaStateTransition.SagaStatusChanged.class);
                SagaStateTransition.SagaStatusChanged c = ((SagaStateTransition.SagaStatusChanged) stateTransition);
                assertThat(c.sagaStatus).isEqualTo(SagaStatus.Failed);
            }
        });
        scc.sagaStateTransitionVerifier.verifyNoRecords();

        scc.sagaStateVerifier.verifyMultiple(2, (i, id, state) -> {
            if (i == 0) {
                assertThat(state.sequence.getSeq()).isEqualTo(7);
                assertThat(state.status).isEqualTo(SagaStatus.InFailure);
                assertThat(state.actions.get(addFundsId1).status).isEqualTo(ActionStatus.Undone);
                assertThat(state.actions.get(transferFundsId).status).isEqualTo(ActionStatus.Failed);
            } else if (i == 1) {
                assertThat(state.sequence.getSeq()).isEqualTo(8);
                assertThat(state.status).isEqualTo(SagaStatus.Failed);
                assertThat(state.actions.get(addFundsId1).status).isEqualTo(ActionStatus.Undone);
                assertThat(state.actions.get(transferFundsId).status).isEqualTo(ActionStatus.Failed);
            }
        });
        scc.sagaStateVerifier.verifyNoRecords();

        scc.sagaResponseVerifier.verifySingle((id, response) -> {
            assertThat(response.sagaId).isEqualTo(saga.sagaId);
            assertThat(response.result.isSuccess()).isFalse();
            assertThat(response.result.failureReasons()).contains(NonEmptyList.of(sagaError));
        });
    }

    @Test
    void testParallelSuccessful() {
        SagaCoordinatorContext scc = new SagaCoordinatorContext();

        Saga<SpecificRecord> saga = getParallelSaga();
        Map<ActionId, CommandIds> commandIds = getCommandIds(saga);
        scc.sagaRequestPublisher.publish(saga.sagaId, new SagaRequest<>(SagaId.random() , saga));

        scc.actionRequestVerifier.verifyMultiple(2, (i, id, actionRequest) -> {
            assertThat(id).isEqualTo(saga.sagaId);
            assertThat(actionRequest.actionType).isEqualTo("addFunds");
            assertThat(actionRequest.actionId).isIn(addFundsId1, addFundsId2);
        });
        scc.actionRequestVerifier.verifyNoRecords();

        scc.sagaStateTransitionVerifier.verifyMultiple(2, (i, id, stateTransition) -> {
        });
        scc.sagaStateTransitionVerifier.verifyNoRecords();

        scc.sagaStateVerifier.verifyMultiple(2, (i, id, state) -> {
        });
        scc.sagaStateVerifier.verifyNoRecords();

        scc.actionResponsePublisher.publish(saga.sagaId, new ActionResponse(saga.sagaId, addFundsId1, commandIds.get(addFundsId1).action, Result.success(true)));
        scc.actionResponsePublisher.publish(saga.sagaId, new ActionResponse(saga.sagaId, addFundsId2, commandIds.get(addFundsId2).action, Result.success(true)));

        scc.sagaStateTransitionVerifier.verifyMultiple(3, (i, id, stateTransition) -> {
        });
        scc.sagaStateTransitionVerifier.verifyNoRecords();

        scc.sagaStateVerifier.verifyMultiple(3, (i, id, state) -> {
        });
        scc.sagaStateVerifier.verifyNoRecords();
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
        scc.sagaRequestPublisher.publish(saga.sagaId, new SagaRequest<>(SagaId.random(), saga));

        scc.actionRequestVerifier.verifyMultiple(2, (i, id, actionRequest) -> {
            assertThat(id).isEqualTo(saga.sagaId);
            assertThat(actionRequest.actionType).isEqualTo("addFunds");
            assertThat(actionRequest.actionId).isIn(addFundsId1, addFundsId2);
        });
        scc.actionRequestVerifier.verifyNoRecords();

        scc.sagaStateTransitionVerifier.verifyMultiple(2, (i, id, stateTransition) -> {
        });
        scc.sagaStateTransitionVerifier.verifyNoRecords();

        scc.sagaStateVerifier.verifyMultiple(2, (i, id, state) -> {
        });
        scc.sagaStateVerifier.verifyNoRecords();

        scc.actionResponsePublisher.publish(saga.sagaId, new ActionResponse(saga.sagaId, addFundsId1, commandIds.get(addFundsId1).action, Result.success(true)));
        SagaError sagaError = SagaError.of(SagaError.Reason.CommandError, "Oh noes");
        scc.actionResponsePublisher.publish(saga.sagaId, new ActionResponse(saga.sagaId, addFundsId2, commandIds.get(addFundsId2).action, Result.failure(sagaError)));

        scc.sagaStateTransitionVerifier.verifyMultiple(4, (i, id, stateTransition) -> {
        });
        scc.sagaStateTransitionVerifier.verifyNoRecords();

        scc.sagaStateVerifier.verifyMultiple(4, (i, id, state) -> {
        });
        scc.sagaStateVerifier.verifyNoRecords();

        // undo non-failing action
        scc.actionRequestVerifier.verifySingle((id, actionRequest) -> {
            assertThat(id).isEqualTo(saga.sagaId);
            assertThat(actionRequest.sagaId).isEqualTo(saga.sagaId);
            assertThat(actionRequest.actionType).isEqualTo("addFunds");
            assertThat(actionRequest.actionId).isEqualTo(addFundsId1);
            assertThat(actionRequest.actionCommand.commandId).isEqualTo(commandIds.get(addFundsId1).undoAction);
        });
        scc.actionRequestVerifier.verifyNoRecords();

        // undo successful
        scc.actionResponsePublisher.publish(saga.sagaId, new ActionResponse(saga.sagaId, addFundsId1, commandIds.get(addFundsId1).undoAction, Result.success(true)));

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
        scc.sagaRequestPublisher.publish(saga.sagaId, new SagaRequest<>(SagaId.random(), saga));

        scc.actionRequestVerifier.verifyMultiple(3, (i, id, actionRequest) -> {
            assertThat(id).isEqualTo(saga.sagaId);
            assertThat(actionRequest.actionId).isIn(addFundsId1, addFundsId2, transferFundsId);
        });
        scc.actionRequestVerifier.verifyNoRecords();

        scc.sagaStateTransitionVerifier.drainAll();
        scc.sagaStateVerifier.drainAll();

        scc.actionResponsePublisher.publish(saga.sagaId, new ActionResponse(saga.sagaId, addFundsId1, commandIds.get(addFundsId1).undoAction, Result.success(true)));

        scc.sagaStateVerifier.verifySingle((id, sagaState) -> {
            assertThat(sagaState.status).isEqualTo(SagaStatus.InProgress);
        });
        scc.sagaStateVerifier.verifyNoRecords();

        SagaError sagaError = SagaError.of(SagaError.Reason.CommandError, "Oh noes");
        scc.actionResponsePublisher.publish(saga.sagaId, new ActionResponse(saga.sagaId, addFundsId2, commandIds.get(addFundsId2).undoAction, Result.failure(sagaError)));

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
        scc.sagaStateVerifier.verifyNoRecords();
        scc.actionRequestVerifier.verifyNoRecords();

        // final action completes
        scc.actionResponsePublisher.publish(saga.sagaId, new ActionResponse(saga.sagaId, transferFundsId, commandIds.get(transferFundsId).action, Result.success(true)));


        List<Saga<SpecificRecord>> s2 = scc.sagaStateVerifier.verifyMultiple(3, (i, id, state) -> {
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
                assertThat(state.actions.get(addFundsId1).status).isEqualTo(ActionStatus.InUndo);
                assertThat(state.actions.get(addFundsId2).status).isEqualTo(ActionStatus.Failed);
                assertThat(state.actions.get(transferFundsId).status).isEqualTo(ActionStatus.InUndo);
            }
        });
        scc.sagaStateVerifier.verifyNoRecords();

        // undo non-failing actions
        scc.actionRequestVerifier.verifyMultiple(2, (i, id, actionRequest) -> {
            assertThat(id).isEqualTo(saga.sagaId);
            assertThat(actionRequest.sagaId).isEqualTo(saga.sagaId);
            assertThat(actionRequest.actionType).isIn("addFunds", "transferFunds");
            assertThat(actionRequest.actionCommand.commandId).isIn(commandIds.get(addFundsId1).undoAction, commandIds.get(transferFundsId).undoAction);
        });
        scc.actionRequestVerifier.verifyNoRecords();

        // undo successful
        scc.actionResponsePublisher.publish(saga.sagaId, new ActionResponse(saga.sagaId, addFundsId1, commandIds.get(addFundsId1).undoAction, Result.success(true)));
        scc.actionResponsePublisher.publish(saga.sagaId, new ActionResponse(saga.sagaId, transferFundsId, commandIds.get(transferFundsId).undoAction, Result.success(true)));

        scc.sagaResponseVerifier.verifySingle((id, response) -> {
            assertThat(response.sagaId).isEqualTo(saga.sagaId);
            assertThat(response.result.isSuccess()).isFalse();
            assertThat(response.result.failureReasons()).contains(NonEmptyList.of(sagaError));
        });
    }
}