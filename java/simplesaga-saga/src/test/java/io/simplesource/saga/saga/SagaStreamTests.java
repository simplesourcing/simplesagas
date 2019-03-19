package io.simplesource.saga.saga;

import io.simplesource.data.NonEmptyList;
import io.simplesource.data.Result;
import io.simplesource.kafka.spec.WindowSpec;
import io.simplesource.saga.dsl.SagaDsl;
import io.simplesource.saga.model.action.ActionCommand;
import io.simplesource.saga.model.action.ActionStatus;
import io.simplesource.saga.model.messages.*;
import io.simplesource.saga.model.saga.Saga;
import io.simplesource.saga.model.saga.SagaError;
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

import java.util.UUID;

import static io.simplesource.saga.dsl.SagaDsl.inParallel;
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
        final RecordPublisher<UUID, SagaRequest<SpecificRecord>> sagaRequestPublisher;
        final RecordPublisher<UUID, ActionResponse> actionResponsePublisher;

        // verifiers
        final RecordVerifier<UUID, ActionRequest<SpecificRecord>> actionRequestVerifier;
        final RecordVerifier<UUID, SagaStateTransition> sagaStateTransitionVerifier;
        final RecordVerifier<UUID, Saga<SpecificRecord>> sagaStateVerifier;
        final RecordVerifier<UUID, SagaResponse> sagaResponseVerifier;

        SagaCoordinatorContext() {
            TopicNamer sagaTopicNamer = TopicNamer.forPrefix(Constants.sagaTopicPrefix, Constants.sagaBaseName);
            TopicNamer actionTopicNamer = TopicNamer.forPrefix(Constants.actionTopicPrefix, Constants.sagaActionBaseName);

            SagaApp<SpecificRecord> sagaApp = new SagaApp<>(
                    new SagaSpec<>(sagaSerdes, new WindowSpec(60)),
                    TopicUtils.buildSteps(Constants.sagaTopicPrefix, Constants.sagaBaseName));
            sagaApp.addActionProcessor(
                    new ActionProcessorSpec<>(actionSerdes),
                    TopicUtils.buildSteps(Constants.actionTopicPrefix, Constants.sagaActionBaseName));

            Topology topology = sagaApp.buildTopology();
            testContext = TestContextBuilder.of(topology).build();

            sagaRequestPublisher = testContext.publisher(
                    sagaTopicNamer.apply(TopicTypes.SagaTopic.request),
                    sagaSerdes.uuid(),
                    sagaSerdes.request());
            actionResponsePublisher = testContext.publisher(
                    actionTopicNamer.apply(TopicTypes.ActionTopic.response),
                    actionSerdes.uuid(),
                    actionSerdes.response());

            actionRequestVerifier = testContext.verifier(
                    actionTopicNamer.apply(TopicTypes.ActionTopic.request),
                    actionSerdes.uuid(),
                    actionSerdes.request());
            sagaStateTransitionVerifier = testContext.verifier(
                    sagaTopicNamer.apply(TopicTypes.SagaTopic.stateTransition),
                    sagaSerdes.uuid(),
                    sagaSerdes.transition());
            sagaStateVerifier = testContext.verifier(
                    sagaTopicNamer.apply(TopicTypes.SagaTopic.state),
                    sagaSerdes.uuid(),
                    sagaSerdes.state());
            sagaResponseVerifier = testContext.verifier(
                    sagaTopicNamer.apply(TopicTypes.SagaTopic.response),
                    sagaSerdes.uuid(),
                    sagaSerdes.response());

        }
    }

    private UUID action1 = UUID.randomUUID();
    private UUID action2 = UUID.randomUUID();
    private UUID createAccountId1 = UUID.randomUUID();
    private UUID createAccountId2 = UUID.randomUUID();
    private UUID addFundsId1 = UUID.randomUUID();
    private UUID undoFundsId1 = UUID.randomUUID();
    private UUID addFundsId2 = UUID.randomUUID();
    private UUID undoFundsId2 = UUID.randomUUID();
    private UUID transferId = UUID.randomUUID();
    private UUID undoTransferId = UUID.randomUUID();

    Saga<SpecificRecord> getBasicSaga() {
        SagaDsl.SagaBuilder<SpecificRecord> builder = SagaDsl.SagaBuilder.create();

        SagaDsl.SubSaga<SpecificRecord> createAccount = builder.addAction(
                action1,
                "createAccount",
                new ActionCommand<>(createAccountId1, new CreateAccount("id1", "User 1")));

        SagaDsl.SubSaga<SpecificRecord> addFunds = builder.addAction(
                action2,
                "addFunds",
                new ActionCommand<>(addFundsId1, new AddFunds("id1", 1000.0)),
                // this will never undo since it's in the last sub-saga
                new ActionCommand<>(undoFundsId1, new AddFunds("id1", -1000.0)));

        createAccount.andThen(addFunds);

        Result<SagaError, Saga<SpecificRecord>> sagaBuildResult = builder.build();
        assertThat(sagaBuildResult.isSuccess()).isEqualTo(true);

        return sagaBuildResult.getOrElse(null);
    }

    Saga<SpecificRecord> getSagaWithUndo() {
        SagaDsl.SagaBuilder<SpecificRecord> builder = SagaDsl.SagaBuilder.create();

        SagaDsl.SubSaga<SpecificRecord> addFunds = builder.addAction(
                action1,
                "addFunds",
                new ActionCommand<>(addFundsId1, new AddFunds("id1", 1000.0)),
                new ActionCommand<>(undoFundsId1, new AddFunds("id1", -1000.0)));

        SagaDsl.SubSaga<SpecificRecord> transferFunds = builder.addAction(
                action2,
                "transferFunds",
                new ActionCommand<>(transferId, new TransferFunds("id1", "id2", 50.0)),
                new ActionCommand<>(undoTransferId, new TransferFunds("id2", "id1", -50.0)));

        addFunds.andThen(transferFunds);

        Result<SagaError, Saga<SpecificRecord>> sagaBuildResult = builder.build();
        assertThat(sagaBuildResult.isSuccess()).isEqualTo(true);

        return sagaBuildResult.getOrElse(null);
    }

    Saga<SpecificRecord> getParallelSaga() {
        SagaDsl.SagaBuilder<SpecificRecord> builder = SagaDsl.SagaBuilder.create();

        SagaDsl.SubSaga<SpecificRecord> addFunds1 = builder.addAction(
                action1,
                "addFunds",
                new ActionCommand<>(addFundsId1, new AddFunds("id1", 1000.0)),
                new ActionCommand<>(undoFundsId1, new AddFunds("id1", -1000.0)));
        SagaDsl.SubSaga<SpecificRecord> addFunds2 = builder.addAction(
                action2,
                "addFunds",
                new ActionCommand<>(addFundsId2, new AddFunds("id2", 1000.0)),
                new ActionCommand<>(undoFundsId2, new AddFunds("id2", -1000.0)));

        inParallel(addFunds1, addFunds2);

        Result<SagaError, Saga<SpecificRecord>> sagaBuildResult = builder.build();
        assertThat(sagaBuildResult.isSuccess()).isEqualTo(true);

        return sagaBuildResult.getOrElse(null);
    }

    @Test
    void testSuccessfulSaga() {
        SagaCoordinatorContext scc = new SagaCoordinatorContext();

        UUID sagaRequestId = UUID.randomUUID();
        Saga<SpecificRecord> saga = getBasicSaga();
        scc.sagaRequestPublisher().publish(saga.sagaId(), new SagaRequest<>(sagaRequestId, saga));

        scc.actionRequestVerifier().verifySingle((id, actionRequest) -> {
            assertThat(id).isEqualTo(saga.sagaId());
            assertThat(actionRequest.actionType()).isEqualTo("createAccount");
            assertThat(actionRequest.actionId()).isEqualTo(action1);
            assertThat(actionRequest.actionCommand().commandId()).isEqualTo(createAccountId1);
        });
        scc.actionRequestVerifier().verifyNoRecords();

        scc.sagaStateTransitionVerifier().verifyMultiple(2, (i, id, stateTransition) -> {
            if (i == 0) {
                assertThat(stateTransition).isInstanceOf(SagaStateTransition.SetInitialState.class);
                Saga s = ((SagaStateTransition.SetInitialState) stateTransition).sagaState();
                assertThat(s.status()).isEqualTo(SagaStatus.NotStarted);
                assertThat(s.sequence().getSeq()).isEqualTo(0);
                assertThat(s.actions()).containsKeys(action1, action2);
            } else if (i == 1) {
                assertThat(stateTransition).isInstanceOf(SagaStateTransition.TransitionList.class);
                SagaStateTransition.TransitionList transitionList = ((SagaStateTransition.TransitionList) stateTransition);
                assertThat(transitionList.actions().size()).isEqualTo(1);
                assertThat(transitionList.actions().get(0).actionStatus()).isEqualTo(ActionStatus.InProgress);
            }
        });
        scc.sagaStateTransitionVerifier().verifyNoRecords();

        scc.sagaStateVerifier().verifyMultiple(2, (i, id, state) -> {
            if (i == 0) {
                assertThat(state.sequence().getSeq()).isEqualTo(0);
                assertThat(state.status()).isEqualTo(SagaStatus.InProgress);
                assertThat(state.actions().get(action1).status()).isEqualTo(ActionStatus.Pending);
                assertThat(state.actions().get(action2).status()).isEqualTo(ActionStatus.Pending);
            } else if (i == 1) {
                assertThat(state.sequence().getSeq()).isEqualTo(1);
                assertThat(state.status()).isEqualTo(SagaStatus.InProgress);
                assertThat(state.actions().get(action1).status()).isEqualTo(ActionStatus.InProgress);
                assertThat(state.actions().get(action2).status()).isEqualTo(ActionStatus.Pending);
            }
        });
        scc.sagaStateVerifier().verifyNoRecords();

        // create account successful
        scc.actionResponsePublisher().publish(saga.sagaId(), new ActionResponse(saga.sagaId(), action1, createAccountId1, Result.success(true)));

        scc.sagaStateTransitionVerifier.verifyMultiple(2, (i, id, stateTransition) -> {
            if (i == 0) {
                assertThat(stateTransition).isInstanceOf(SagaStateTransition.SagaActionStatusChanged.class);
                SagaStateTransition.SagaActionStatusChanged c = ((SagaStateTransition.SagaActionStatusChanged) stateTransition);
                assertThat(c.actionId()).isEqualTo(action1);
                assertThat(c.actionStatus()).isEqualTo(ActionStatus.Completed);
            } else if (i == 1) {
                assertThat(stateTransition).isInstanceOf(SagaStateTransition.TransitionList.class);
                SagaStateTransition.TransitionList transitionList = ((SagaStateTransition.TransitionList) stateTransition);
                assertThat(transitionList.actions().size()).isEqualTo(1);
                assertThat(transitionList.actions().get(0).actionStatus()).isEqualTo(ActionStatus.InProgress);
            }
        });
        scc.sagaStateTransitionVerifier().verifyNoRecords();

        scc.sagaStateVerifier().verifyMultiple(2, (i, id, state) -> {
            if (i == 0) {
                assertThat(state.sequence().getSeq()).isEqualTo(2);
                assertThat(state.status()).isEqualTo(SagaStatus.InProgress);
                assertThat(state.actions().get(action1).status()).isEqualTo(ActionStatus.Completed);
                assertThat(state.actions().get(action2).status()).isEqualTo(ActionStatus.Pending);
            } else if (i == 1) {
                assertThat(state.sequence().getSeq()).isEqualTo(3);
                assertThat(state.status()).isEqualTo(SagaStatus.InProgress);
                assertThat(state.actions().get(action1).status()).isEqualTo(ActionStatus.Completed);
                assertThat(state.actions().get(action2).status()).isEqualTo(ActionStatus.InProgress);
            }
        });
        scc.sagaStateVerifier().verifyNoRecords();

        scc.actionRequestVerifier().verifySingle((id, actionRequest) -> {
            assertThat(id).isEqualTo(saga.sagaId());
            assertThat(actionRequest.sagaId()).isEqualTo(saga.sagaId());
            assertThat(actionRequest.actionType()).isEqualTo("addFunds");
            assertThat(actionRequest.actionId()).isEqualTo(action2);
            assertThat(actionRequest.actionCommand().commandId()).isEqualTo(addFundsId1);
        });
        scc.actionRequestVerifier().verifyNoRecords();

        // add funds successful
        scc.actionResponsePublisher().publish(saga.sagaId(), new ActionResponse(saga.sagaId(), action2, addFundsId1, Result.success(true)));

        scc.sagaStateTransitionVerifier.verifyMultiple(2, (i, id, stateTransition) -> {
            if (i == 0) {
                assertThat(stateTransition).isInstanceOf(SagaStateTransition.SagaActionStatusChanged.class);
                SagaStateTransition.SagaActionStatusChanged c = ((SagaStateTransition.SagaActionStatusChanged) stateTransition);
                assertThat(c.actionId()).isEqualTo(action2);
                assertThat(c.actionStatus()).isEqualTo(ActionStatus.Completed);
            } else if (i == 1) {
                assertThat(stateTransition).isInstanceOf(SagaStateTransition.SagaStatusChanged.class);
                SagaStateTransition.SagaStatusChanged c = ((SagaStateTransition.SagaStatusChanged) stateTransition);
                assertThat(c.sagaId()).isEqualTo(saga.sagaId());
                assertThat(c.sagaStatus).isEqualTo(SagaStatus.Completed);
            }
        });
        scc.sagaStateTransitionVerifier().verifyNoRecords();

        scc.sagaStateVerifier().verifyMultiple(2, (i, id, state) -> {
            if (i == 0) {
                assertThat(state.sequence().getSeq()).isEqualTo(4);
                assertThat(state.status()).isEqualTo(SagaStatus.InProgress);
                assertThat(state.actions().get(action1).status()).isEqualTo(ActionStatus.Completed);
                assertThat(state.actions().get(action2).status()).isEqualTo(ActionStatus.Completed);
            } else if (i == 1) {
                assertThat(state.sequence().getSeq()).isEqualTo(5);
                assertThat(state.status()).isEqualTo(SagaStatus.Completed);
                assertThat(state.actions().get(action1).status()).isEqualTo(ActionStatus.Completed);
                assertThat(state.actions().get(action2).status()).isEqualTo(ActionStatus.Completed);
            }
        });
        scc.sagaStateVerifier().verifyNoRecords();
        scc.actionRequestVerifier().verifyNoRecords();

        scc.sagaResponseVerifier().verifySingle((id, response) -> {
            assertThat(response.sagaId()).isEqualTo(saga.sagaId());
            assertThat(response.result.isSuccess()).isTrue();
        });
    }

    @Test
    void testShortCircuitOnFailure() {
        SagaCoordinatorContext scc = new SagaCoordinatorContext();

        UUID sagaRequestId = UUID.randomUUID();
        Saga<SpecificRecord> saga = getBasicSaga();
        scc.sagaRequestPublisher().publish(saga.sagaId(), new SagaRequest<>(sagaRequestId, saga));

        // already verified in above test
        scc.actionRequestVerifier().drainAll();
        scc.sagaStateTransitionVerifier().drainAll();
        scc.sagaStateVerifier().drainAll();

        // create account failed
        SagaError sagaError = SagaError.of(SagaError.Reason.CommandError, "Oh noes");
        scc.actionResponsePublisher().publish(saga.sagaId(), new ActionResponse(saga.sagaId(), action1, createAccountId1, Result.failure(sagaError)));

        scc.sagaStateTransitionVerifier().verifyMultiple(2, (i, id, stateTransition) -> {
            if (i == 0) {
                assertThat(stateTransition).isInstanceOf(SagaStateTransition.SagaActionStatusChanged.class);
                SagaStateTransition.SagaActionStatusChanged c = ((SagaStateTransition.SagaActionStatusChanged) stateTransition);
                assertThat(c.actionId()).isEqualTo(action1);
                assertThat(c.actionStatus()).isEqualTo(ActionStatus.Failed);
            } else if (i == 1) {
                assertThat(stateTransition).isInstanceOf(SagaStateTransition.SagaStatusChanged.class);
                SagaStateTransition.SagaStatusChanged c = ((SagaStateTransition.SagaStatusChanged) stateTransition);
                assertThat(c.sagaStatus()).isEqualTo(SagaStatus.Failed);
            }
        });
        scc.sagaStateTransitionVerifier().verifyNoRecords();

        scc.sagaStateVerifier().verifyMultiple(2, (i, id, state) -> {
            if (i == 0) {
                assertThat(state.sequence().getSeq()).isEqualTo(2);
                assertThat(state.status()).isEqualTo(SagaStatus.InProgress);
                assertThat(state.actions().get(action1).status()).isEqualTo(ActionStatus.Failed);
                assertThat(state.actions().get(action2).status()).isEqualTo(ActionStatus.Pending);
            } else if (i == 1) {
                assertThat(state.sequence().getSeq()).isEqualTo(3);
                assertThat(state.status()).isEqualTo(SagaStatus.Failed);
                assertThat(state.actions().get(action1).status()).isEqualTo(ActionStatus.Failed);
                assertThat(state.actions().get(action2).status()).isEqualTo(ActionStatus.Pending);
            }
        });
        scc.sagaStateVerifier().verifyNoRecords();

        scc.actionRequestVerifier().verifyNoRecords();

        scc.sagaResponseVerifier().verifySingle((id, response) -> {
            assertThat(response.sagaId()).isEqualTo(saga.sagaId());
            assertThat(response.result().isSuccess()).isFalse();
            assertThat(response.result().failureReasons()).contains(NonEmptyList.of(sagaError));
        });
    }

    @Test
    void testBypassUndoOnFailureIfNotDefined() {
        SagaCoordinatorContext scc = new SagaCoordinatorContext();

        UUID sagaRequestId = UUID.randomUUID();
        Saga<SpecificRecord> saga = getBasicSaga();
        scc.sagaRequestPublisher().publish(saga.sagaId(), new SagaRequest<>(sagaRequestId, saga));

        // create account successful
        scc.actionResponsePublisher().publish(saga.sagaId(), new ActionResponse(saga.sagaId(), action1, createAccountId1, Result.success(true)));

        scc.actionRequestVerifier().drainAll();
        scc.sagaStateTransitionVerifier().drainAll();
        scc.sagaStateVerifier().drainAll();

        // add funds failed
        SagaError sagaError = SagaError.of(SagaError.Reason.CommandError, "Oh noes");
        scc.actionResponsePublisher().publish(saga.sagaId(), new ActionResponse(saga.sagaId(), action2, addFundsId1, Result.failure(sagaError)));

        scc.sagaStateTransitionVerifier().verifyMultiple(4, (i, id, stateTransition) -> {
            if (i == 0) {
                assertThat(stateTransition).isInstanceOf(SagaStateTransition.SagaActionStatusChanged.class);
                SagaStateTransition.SagaActionStatusChanged c = ((SagaStateTransition.SagaActionStatusChanged) stateTransition);
                assertThat(c.actionId()).isEqualTo(action2);
                assertThat(c.actionStatus()).isEqualTo(ActionStatus.Failed);
            } else if (i == 1) {
                assertThat(stateTransition).isInstanceOf(SagaStateTransition.SagaStatusChanged.class);
                SagaStateTransition.SagaStatusChanged c = ((SagaStateTransition.SagaStatusChanged) stateTransition);
                assertThat(c.sagaStatus()).isEqualTo(SagaStatus.InFailure);
            } else if (i == 2) {
                assertThat(stateTransition).isInstanceOf(SagaStateTransition.TransitionList.class);
                SagaStateTransition.TransitionList transitionList = ((SagaStateTransition.TransitionList) stateTransition);
                assertThat(transitionList.actions().size()).isEqualTo(1);
                assertThat(transitionList.actions().get(0).actionStatus()).isEqualTo(ActionStatus.UndoBypassed);
            } else if (i == 3) {
                assertThat(stateTransition).isInstanceOf(SagaStateTransition.SagaStatusChanged.class);
                SagaStateTransition.SagaStatusChanged c = ((SagaStateTransition.SagaStatusChanged) stateTransition);
                assertThat(c.sagaStatus()).isEqualTo(SagaStatus.Failed);
            }
        });
        scc.sagaStateTransitionVerifier().verifyNoRecords();

        scc.sagaStateVerifier().verifyMultiple(4, (i, id, state) -> {
            if (i == 0) {
                assertThat(state.sequence().getSeq()).isEqualTo(4);
                assertThat(state.status()).isEqualTo(SagaStatus.InProgress);
                assertThat(state.actions().get(action1).status()).isEqualTo(ActionStatus.Completed);
                assertThat(state.actions().get(action2).status()).isEqualTo(ActionStatus.Failed);
            } else if (i == 1) {
                assertThat(state.sequence().getSeq()).isEqualTo(5);
                assertThat(state.status()).isEqualTo(SagaStatus.InFailure);
                assertThat(state.actions().get(action1).status()).isEqualTo(ActionStatus.Completed);
                assertThat(state.actions().get(action2).status()).isEqualTo(ActionStatus.Failed);
            } else if (i == 2) {
                assertThat(state.sequence().getSeq()).isEqualTo(6);
                assertThat(state.status()).isEqualTo(SagaStatus.InFailure);
                assertThat(state.actions().get(action1).status()).isEqualTo(ActionStatus.UndoBypassed);
                assertThat(state.actions().get(action2).status()).isEqualTo(ActionStatus.Failed);
            } else if (i == 3) {
                assertThat(state.sequence().getSeq()).isEqualTo(7);
                assertThat(state.status()).isEqualTo(SagaStatus.Failed);
                assertThat(state.actions().get(action1).status()).isEqualTo(ActionStatus.UndoBypassed);
                assertThat(state.actions().get(action2).status()).isEqualTo(ActionStatus.Failed);
            }
        });
        scc.sagaStateVerifier().verifyNoRecords();

        scc.actionRequestVerifier().verifyNoRecords();

        scc.sagaResponseVerifier().verifySingle((id, response) -> {
            assertThat(response.sagaId()).isEqualTo(saga.sagaId());
            assertThat(response.result().isSuccess()).isFalse();
            assertThat(response.result().failureReasons()).contains(NonEmptyList.of(sagaError));
        });
    }

    @Test
    void testUndoCommandOnFailure() {
        SagaCoordinatorContext scc = new SagaCoordinatorContext();

        UUID sagaRequestId = UUID.randomUUID();
        Saga<SpecificRecord> saga = getSagaWithUndo();
        scc.sagaRequestPublisher().publish(saga.sagaId(), new SagaRequest<>(sagaRequestId, saga));

        // add funds successful
        scc.actionResponsePublisher().publish(saga.sagaId(), new ActionResponse(saga.sagaId(), action1, addFundsId1, Result.success(true)));

        scc.actionRequestVerifier().drainAll();
        scc.sagaStateTransitionVerifier().drainAll();
        scc.sagaStateVerifier().drainAll();

        // transfer funds failed
        SagaError sagaError = SagaError.of(SagaError.Reason.CommandError, "Oh noes");
        scc.actionResponsePublisher().publish(saga.sagaId(), new ActionResponse(saga.sagaId(), action2, transferId, Result.failure(sagaError)));

        scc.sagaStateTransitionVerifier().verifyMultiple(3, (i, id, stateTransition) -> {
            if (i == 0) {
                assertThat(stateTransition).isInstanceOf(SagaStateTransition.SagaActionStatusChanged.class);
                SagaStateTransition.SagaActionStatusChanged c = ((SagaStateTransition.SagaActionStatusChanged) stateTransition);
                assertThat(c.actionId()).isEqualTo(action2);
                assertThat(c.actionStatus()).isEqualTo(ActionStatus.Failed);
            } else if (i == 1) {
                assertThat(stateTransition).isInstanceOf(SagaStateTransition.SagaStatusChanged.class);
                SagaStateTransition.SagaStatusChanged c = ((SagaStateTransition.SagaStatusChanged) stateTransition);
                assertThat(c.sagaStatus()).isEqualTo(SagaStatus.InFailure);
            } else if (i == 2) {
                assertThat(stateTransition).isInstanceOf(SagaStateTransition.TransitionList.class);
                SagaStateTransition.TransitionList transitionList = ((SagaStateTransition.TransitionList) stateTransition);
                assertThat(transitionList.actions().size()).isEqualTo(1);
                assertThat(transitionList.actions().get(0).actionStatus()).isEqualTo(ActionStatus.InUndo);
            }
        });
        scc.sagaStateTransitionVerifier().verifyNoRecords();

        scc.sagaStateVerifier().verifyMultiple(3, (i, id, state) -> {
            if (i == 0) {
                assertThat(state.sequence().getSeq()).isEqualTo(4);
                assertThat(state.status()).isEqualTo(SagaStatus.InProgress);
                assertThat(state.actions().get(action1).status()).isEqualTo(ActionStatus.Completed);
                assertThat(state.actions().get(action2).status()).isEqualTo(ActionStatus.Failed);
            } else if (i == 1) {
                assertThat(state.sequence().getSeq()).isEqualTo(5);
                assertThat(state.status()).isEqualTo(SagaStatus.InFailure);
                assertThat(state.actions().get(action1).status()).isEqualTo(ActionStatus.Completed);
                assertThat(state.actions().get(action2).status()).isEqualTo(ActionStatus.Failed);
            } else if (i == 2) {
                assertThat(state.sequence().getSeq()).isEqualTo(6);
                assertThat(state.status()).isEqualTo(SagaStatus.InFailure);
                assertThat(state.actions().get(action1).status()).isEqualTo(ActionStatus.InUndo);
                assertThat(state.actions().get(action2).status()).isEqualTo(ActionStatus.Failed);
            }
        });
        scc.sagaStateVerifier().verifyNoRecords();

        scc.actionRequestVerifier().verifySingle((id, actionRequest) -> {
            assertThat(id).isEqualTo(saga.sagaId());
            assertThat(actionRequest.sagaId()).isEqualTo(saga.sagaId());
            assertThat(actionRequest.actionType()).isEqualTo("addFunds");
            assertThat(actionRequest.actionId()).isEqualTo(action1);
            assertThat(actionRequest.actionCommand().commandId()).isEqualTo(undoFundsId1);
        });
        scc.actionRequestVerifier().verifyNoRecords();

        // undo add funds successful
        scc.actionResponsePublisher().publish(saga.sagaId(), new ActionResponse(saga.sagaId(), action1, undoFundsId1, Result.success(true)));

        scc.sagaStateTransitionVerifier().verifyMultiple(2, (i, id, stateTransition) -> {
            if (i == 0) {
                assertThat(stateTransition).isInstanceOf(SagaStateTransition.SagaActionStatusChanged.class);
                SagaStateTransition.SagaActionStatusChanged c = ((SagaStateTransition.SagaActionStatusChanged) stateTransition);
                assertThat(c.actionId()).isEqualTo(action1);
                assertThat(c.actionStatus()).isEqualTo(ActionStatus.Completed);
            } else if (i == 1) {
                assertThat(stateTransition).isInstanceOf(SagaStateTransition.SagaStatusChanged.class);
                SagaStateTransition.SagaStatusChanged c = ((SagaStateTransition.SagaStatusChanged) stateTransition);
                assertThat(c.sagaStatus()).isEqualTo(SagaStatus.Failed);
            }
        });
        scc.sagaStateTransitionVerifier().verifyNoRecords();

        scc.sagaStateVerifier().verifyMultiple(2, (i, id, state) -> {
            if (i == 0) {
                assertThat(state.sequence().getSeq()).isEqualTo(7);
                assertThat(state.status()).isEqualTo(SagaStatus.InFailure);
                assertThat(state.actions().get(action1).status()).isEqualTo(ActionStatus.Undone);
                assertThat(state.actions().get(action2).status()).isEqualTo(ActionStatus.Failed);
            } else if (i == 1) {
                assertThat(state.sequence().getSeq()).isEqualTo(8);
                assertThat(state.status()).isEqualTo(SagaStatus.Failed);
                assertThat(state.actions().get(action1).status()).isEqualTo(ActionStatus.Undone);
                assertThat(state.actions().get(action2).status()).isEqualTo(ActionStatus.Failed);
            }
        });
        scc.sagaStateVerifier().verifyNoRecords();

        scc.sagaResponseVerifier().verifySingle((id, response) -> {
            assertThat(response.sagaId()).isEqualTo(saga.sagaId());
            assertThat(response.result().isSuccess()).isFalse();
            assertThat(response.result().failureReasons()).contains(NonEmptyList.of(sagaError));
        });
    }

    @Test
    void testParallelSuccessful() {
        SagaCoordinatorContext scc = new SagaCoordinatorContext();

        Saga<SpecificRecord> saga = getParallelSaga();
        scc.sagaRequestPublisher().publish(saga.sagaId(), new SagaRequest<>(UUID.randomUUID(), saga));

        scc.actionRequestVerifier().verifyMultiple(2, (i, id, actionRequest) -> {
            assertThat(id).isEqualTo(saga.sagaId());
            assertThat(actionRequest.actionType()).isEqualTo("addFunds");
            assertThat(actionRequest.actionId()).isIn(action1, action2);
        });
        scc.actionRequestVerifier().verifyNoRecords();

        scc.sagaStateTransitionVerifier().verifyMultiple(2, (i, id, stateTransition) -> {
        });
        scc.sagaStateTransitionVerifier().verifyNoRecords();

        scc.sagaStateVerifier().verifyMultiple(2, (i, id, state) -> {
        });
        scc.sagaStateVerifier().verifyNoRecords();

        scc.actionResponsePublisher().publish(saga.sagaId(), new ActionResponse(saga.sagaId(), action1, createAccountId1, Result.success(true)));
        scc.actionResponsePublisher().publish(saga.sagaId(), new ActionResponse(saga.sagaId(), action2, createAccountId2, Result.success(true)));

        scc.sagaStateTransitionVerifier().verifyMultiple(3, (i, id, stateTransition) -> {
        });
        scc.sagaStateTransitionVerifier().verifyNoRecords();

        scc.sagaStateVerifier().verifyMultiple(3, (i, id, state) -> {
        });
        scc.sagaStateVerifier().verifyNoRecords();
        scc.sagaResponseVerifier().verifySingle((id, response) -> {
            assertThat(response.sagaId()).isEqualTo(saga.sagaId());
            assertThat(response.result().isSuccess()).isTrue();
        });
    }

    @Test
    void testFailureInParallel() {
        SagaCoordinatorContext scc = new SagaCoordinatorContext();

        Saga<SpecificRecord> saga = getParallelSaga();
        scc.sagaRequestPublisher().publish(saga.sagaId(), new SagaRequest<>(UUID.randomUUID(), saga));

        scc.actionRequestVerifier().verifyMultiple(2, (i, id, actionRequest) -> {
            assertThat(id).isEqualTo(saga.sagaId());
            assertThat(actionRequest.actionType()).isEqualTo("addFunds");
            assertThat(actionRequest.actionId()).isIn(action1, action2);
        });
        scc.actionRequestVerifier().verifyNoRecords();

        scc.sagaStateTransitionVerifier().verifyMultiple(2, (i, id, stateTransition) -> {
        });
        scc.sagaStateTransitionVerifier().verifyNoRecords();

        scc.sagaStateVerifier().verifyMultiple(2, (i, id, state) -> {
        });
        scc.sagaStateVerifier().verifyNoRecords();

        scc.actionResponsePublisher().publish(saga.sagaId(), new ActionResponse(saga.sagaId(), action1, addFundsId1, Result.success(true)));
        SagaError sagaError = SagaError.of(SagaError.Reason.CommandError, "Oh noes");
        scc.actionResponsePublisher().publish(saga.sagaId(), new ActionResponse(saga.sagaId(), action2, addFundsId2, Result.failure(sagaError)));

        scc.sagaStateTransitionVerifier().verifyMultiple(4, (i, id, stateTransition) -> {
        });
        scc.sagaStateTransitionVerifier().verifyNoRecords();

        scc.sagaStateVerifier().verifyMultiple(4, (i, id, state) -> {
        });
        scc.sagaStateVerifier().verifyNoRecords();

        // undo non-failing action
        scc.actionRequestVerifier().verifySingle((id, actionRequest) -> {
            assertThat(id).isEqualTo(saga.sagaId());
            assertThat(actionRequest.sagaId()).isEqualTo(saga.sagaId());
            assertThat(actionRequest.actionType()).isEqualTo("addFunds");
            assertThat(actionRequest.actionId()).isEqualTo(action1);
            assertThat(actionRequest.actionCommand().commandId()).isEqualTo(undoFundsId1);
        });
        scc.actionRequestVerifier().verifyNoRecords();

        // undo successful
        scc.actionResponsePublisher().publish(saga.sagaId(), new ActionResponse(saga.sagaId(), action1, undoFundsId1, Result.success(true)));

        scc.sagaResponseVerifier().verifySingle((id, response) -> {
            assertThat(response.sagaId()).isEqualTo(saga.sagaId());
            assertThat(response.result().isSuccess()).isFalse();
            assertThat(response.result().failureReasons()).contains(NonEmptyList.of(sagaError));
        });
    }
}