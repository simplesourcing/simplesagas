package io.simplesource.saga.saga.app;

import io.simplesource.api.CommandId;
import io.simplesource.data.Sequence;
import io.simplesource.saga.model.action.ActionCommand;
import io.simplesource.saga.model.action.ActionStatus;
import io.simplesource.saga.model.action.SagaAction;
import io.simplesource.saga.model.messages.SagaStateTransition;
import io.simplesource.saga.model.saga.Saga;
import io.simplesource.saga.model.saga.SagaError;
import io.simplesource.saga.model.saga.SagaStatus;
import io.simplesource.saga.saga.avro.generated.test.AddFunds;
import io.simplesource.saga.saga.avro.generated.test.CreateAccount;
import org.apache.avro.specific.SpecificRecord;
import org.junit.jupiter.api.Test;
import scala.Function1;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

class SagaUtilsTest {

    @Test
    void testFailedAction() {
        assertThat(SagaUtils.failedAction(getTestSaga(ActionStatus.Failed))).isTrue();
        assertThat(SagaUtils.failedAction(getTestSaga(ActionStatus.Completed, ActionStatus.Failed))).isTrue();
        assertThat(SagaUtils.failedAction(getTestSaga(ActionStatus.Completed, ActionStatus.InProgress))).isFalse();
    }

    @Test
    void testActionInProgress() {
        assertThat(SagaUtils.actionInProgress(getTestSaga(ActionStatus.InProgress))).isTrue();
        assertThat(SagaUtils.actionInProgress(getTestSaga(ActionStatus.Completed, ActionStatus.InProgress))).isTrue();
        assertThat(SagaUtils.actionInProgress(getTestSaga(ActionStatus.Failed))).isFalse();
        assertThat(SagaUtils.actionInProgress(getTestSaga(ActionStatus.Completed, ActionStatus.Failed))).isFalse();
    }

    @Test
    void testSagaCompleted() {
        assertThat(SagaUtils.sagaCompleted(getTestSaga(ActionStatus.Completed))).isTrue();
        assertThat(SagaUtils.sagaCompleted(getTestSaga(ActionStatus.InProgress))).isFalse();
        assertThat(SagaUtils.sagaCompleted(getTestSaga(ActionStatus.Completed, ActionStatus.Failed))).isFalse();
    }

    @Test
    void testSagaFailurePending() {
        assertThat(SagaUtils.sagaFailurePending(getTestSaga(ActionStatus.Failed, ActionStatus.InProgress))).isTrue();
        assertThat(SagaUtils.sagaFailurePending(getTestSaga(ActionStatus.InProgress))).isFalse();
        assertThat(SagaUtils.sagaFailurePending(getTestSaga(ActionStatus.Failed, ActionStatus.Completed))).isFalse();
    }

    @Test
    void testSagaInFailure() {
        assertThat(SagaUtils.sagaInFailure(getTestSaga(ActionStatus.Failed, ActionStatus.InUndo))).isTrue();
        assertThat(SagaUtils.sagaInFailure(getTestSaga(ActionStatus.Failed, ActionStatus.Completed))).isTrue();
        assertThat(SagaUtils.sagaInFailure(getTestSaga(ActionStatus.Failed, ActionStatus.InProgress))).isFalse();
        assertThat(SagaUtils.sagaInFailure(getTestSaga(ActionStatus.InProgress))).isFalse();
    }

    @Test
    void testSagaFailed() {
        assertThat(SagaUtils.sagaFailed(getTestSaga(ActionStatus.Failed))).isTrue();
        assertThat(SagaUtils.sagaFailed(getTestSaga(ActionStatus.Failed, ActionStatus.Completed))).isFalse();
        assertThat(SagaUtils.sagaFailed(getTestSaga(ActionStatus.Failed, ActionStatus.InProgress))).isFalse();
        assertThat(SagaUtils.sagaFailed(getTestSaga(ActionStatus.InProgress))).isFalse();
    }

    @Test
    void testNextActionsReturnsAllPending() {
        UUID action1 = UUID.randomUUID();
        UUID action2 = UUID.randomUUID();
        UUID action3 = UUID.randomUUID();
        Saga<SpecificRecord> saga = new SagaBuilder<SpecificRecord>()
                .id(UUID.randomUUID())
                .status(SagaStatus.InProgress)
                .action(builder -> builder
                        .id(action1)
                        .status(ActionStatus.Pending)
                        .command(new ActionCommand<>(CommandId.random(), new CreateAccount("id1", "username1")))
                        .build())
                .action(builder -> builder
                        .id(action2)
                        .status(ActionStatus.Completed)
                        .command(new ActionCommand<>(CommandId.random(), new CreateAccount("id2", "username2")))
                        .build())
                .action(builder -> builder
                        .id(action3)
                        .status(ActionStatus.Pending)
                        .command(new ActionCommand<>(CommandId.random(), new CreateAccount("id3", "username3")))
                        .build())
                .build();
        assertThat(SagaUtils.getNextActions(saga).stream().map(a -> a.actionId)).containsExactlyInAnyOrder(action1, action3);
    }

    @Test
    void testNextActionsHandlesDependencies() {
        UUID action1 = UUID.randomUUID();
        UUID action2 = UUID.randomUUID();
        UUID action3 = UUID.randomUUID();
        Saga<SpecificRecord> saga = new SagaBuilder<SpecificRecord>()
                .id(UUID.randomUUID())
                .status(SagaStatus.InProgress)
                .action(builder -> builder
                        .id(action1)
                        .status(ActionStatus.Pending)
                        .command(new ActionCommand<>(CommandId.random(), new CreateAccount("id1", "username1")))
                        .dependency(action3)
                        .build())
                .action(builder -> builder
                        .id(action2)
                        .status(ActionStatus.Completed)
                        .command(new ActionCommand<>(CommandId.random(), new CreateAccount("id2", "username2")))
                        .build())
                .action(builder -> builder
                        .id(action3)
                        .status(ActionStatus.Pending)
                        .command(new ActionCommand<>(CommandId.random(), new CreateAccount("id3", "username3")))
                        .build())
                .build();
        assertThat(SagaUtils.getNextActions(saga).stream().map(a -> a.actionId)).containsOnly(action3);
    }

    @Test
    void testNextActionsReturnsNothingIfCompleted() {
        UUID action1 = UUID.randomUUID();
        Saga<SpecificRecord> saga = new SagaBuilder<SpecificRecord>()
                .id(UUID.randomUUID())
                .status(SagaStatus.Completed)
                .action(builder -> builder
                        .id(action1)
                        .status(ActionStatus.Pending)
                        .command(new ActionCommand<>(CommandId.random(), new CreateAccount("id1", "username1")))
                        .build())
                .build();
        assertThat(SagaUtils.getNextActions(saga)).isEmpty();
    }

    @Test
    void testNextActionsReturnsUndoBypassedActions() {
        UUID action1 = UUID.randomUUID();
        UUID action2 = UUID.randomUUID();
        UUID action3 = UUID.randomUUID();
        Saga<SpecificRecord> saga = new SagaBuilder<SpecificRecord>()
                .id(UUID.randomUUID())
                .status(SagaStatus.InFailure)
                .action(builder -> builder
                        .id(action1)
                        .status(ActionStatus.Completed)
                        .command(new ActionCommand<>(CommandId.random(), new CreateAccount("id1", "username1")))
                        .build())
                .action(builder -> builder
                        .id(action2)
                        .status(ActionStatus.Failed)
                        .command(new ActionCommand<>(CommandId.random(), new CreateAccount("id2", "username2")))
                        .build())
                .action(builder -> builder
                        .id(action3)
                        .status(ActionStatus.Completed)
                        .command(new ActionCommand<>(CommandId.random(), new CreateAccount("id3", "username3")))
                        .build())
                .build();
        assertThat(SagaUtils.getNextActions(saga).stream().map(a -> a.actionId)).containsExactlyInAnyOrder(action1, action3);
        assertThat(SagaUtils.getNextActions(saga).stream().map(a -> a.status)).containsOnly(ActionStatus.UndoBypassed);
    }

    @Test
    void testNextActionsReturnsUndoActions() {
        UUID action1 = UUID.randomUUID();
        UUID action2 = UUID.randomUUID();
        UUID action3 = UUID.randomUUID();
        CommandId undoCommand1 = CommandId.random();
        CommandId undoCommand2 = CommandId.random();
        CommandId undoCommand3 = CommandId.random();
        Saga<SpecificRecord> saga = new SagaBuilder<SpecificRecord>()
                .id(UUID.randomUUID())
                .status(SagaStatus.InFailure)
                .action(builder -> builder
                        .id(action1)
                        .status(ActionStatus.Completed)
                        .command(new ActionCommand<>(CommandId.random(), new AddFunds("id1", 10.0)))
                        .undoCommand(new ActionCommand<>(undoCommand1, new AddFunds("id1", -10.0)))
                        .build())
                .action(builder -> builder
                        .id(action2)
                        .status(ActionStatus.Failed)
                        .command(new ActionCommand<>(CommandId.random(), new AddFunds("id2", 10.0)))
                        .undoCommand(new ActionCommand<>(undoCommand2, new AddFunds("id2", -10.0)))
                        .build())
                .action(builder -> builder
                        .id(action3)
                        .status(ActionStatus.Completed)
                        .command(new ActionCommand<>(CommandId.random(), new AddFunds("id3", 10.0)))
                        .undoCommand(new ActionCommand<>(undoCommand3, new AddFunds("id3", -10.0)))
                        .build())
                .build();
        assertThat(SagaUtils.getNextActions(saga).stream().map(a -> a.actionId)).containsExactlyInAnyOrder(action1, action3);
        assertThat(SagaUtils.getNextActions(saga).stream().map(a -> a.status)).containsOnly(ActionStatus.InUndo);
        assertThat(SagaUtils.getNextActions(saga).stream().map(a -> a.command.get().commandId)).containsExactlyInAnyOrder(undoCommand1, undoCommand3);
    }

    @Test
    void testNextActionsHandlesDependenciesForUndo() {
        UUID action1 = UUID.randomUUID();
        UUID action2 = UUID.randomUUID();
        UUID action3 = UUID.randomUUID();
        CommandId undoCommand1 = CommandId.random();
        CommandId undoCommand2 = CommandId.random();
        CommandId undoCommand3 = CommandId.random();
        Saga<SpecificRecord> saga = new SagaBuilder<SpecificRecord>()
                .id(UUID.randomUUID())
                .status(SagaStatus.InFailure)
                .action(builder -> builder
                        .id(action1)
                        .status(ActionStatus.Completed)
                        .command(new ActionCommand<>(CommandId.random(), new AddFunds("id1", 10.0)))
                        .undoCommand(new ActionCommand<>(undoCommand1, new AddFunds("id1", -10.0)))
                        .dependency(action3)
                        .build())
                .action(builder -> builder
                        .id(action2)
                        .status(ActionStatus.Failed)
                        .command(new ActionCommand<>(CommandId.random(), new AddFunds("id2", 10.0)))
                        .undoCommand(new ActionCommand<>(undoCommand2, new AddFunds("id2", -10.0)))
                        .build())
                .action(builder -> builder
                        .id(action3)
                        .status(ActionStatus.Completed)
                        .command(new ActionCommand<>(CommandId.random(), new AddFunds("id3", 10.0)))
                        .undoCommand(new ActionCommand<>(undoCommand3, new AddFunds("id3", -10.0)))
                        .build())
                .build();
        assertThat(SagaUtils.getNextActions(saga).stream().map(a -> a.actionId)).containsOnly(action1);
        assertThat(SagaUtils.getNextActions(saga).stream().map(a -> a.status)).containsOnly(ActionStatus.InUndo);
        assertThat(SagaUtils.getNextActions(saga).stream().map(a -> a.command.get().commandId)).containsOnly(undoCommand1);
    }

    @Test
    void testApplyTransitionInitialState() {
        UUID action1 = UUID.randomUUID();
        UUID action2 = UUID.randomUUID();
        UUID action3 = UUID.randomUUID();
        Saga<SpecificRecord> saga = new SagaBuilder<SpecificRecord>()
                .id(UUID.randomUUID())
                .status(SagaStatus.NotStarted)
                .action(builder -> builder
                        .id(action1)
                        .status(ActionStatus.Pending)
                        .command(new ActionCommand<>(CommandId.random(), new CreateAccount("id1", "username1")))
                        .build())
                .action(builder -> builder
                        .id(action2)
                        .status(ActionStatus.Completed)
                        .command(new ActionCommand<>(CommandId.random(), new CreateAccount("id2", "username2")))
                        .build())
                .action(builder -> builder
                        .id(action3)
                        .status(ActionStatus.Pending)
                        .command(new ActionCommand<>(CommandId.random(), new CreateAccount("id3", "username3")))
                        .build())
                .build();
        SagaStateTransition transition = new SagaStateTransition.SetInitialState<>(saga);
        Saga<SpecificRecord> result = SagaUtils.applyTransition(transition, saga);
        assertThat(result.status).isEqualTo(SagaStatus.InProgress);
        assertThat(result.actions).isEqualTo(saga.actions);
    }

    @Test
    void testApplyTransitionSagaActionStatusChanged() {
        UUID sagaId = UUID.randomUUID();
        UUID action1 = UUID.randomUUID();
        UUID action2 = UUID.randomUUID();
        UUID action3 = UUID.randomUUID();
        Saga<SpecificRecord> saga = new SagaBuilder<SpecificRecord>()
                .id(sagaId)
                .status(SagaStatus.InProgress)
                .action(builder -> builder
                        .id(action1)
                        .status(ActionStatus.Pending)
                        .command(new ActionCommand<>(CommandId.random(), new CreateAccount("id1", "username1")))
                        .build())
                .action(builder -> builder
                        .id(action2)
                        .status(ActionStatus.Completed)
                        .command(new ActionCommand<>(CommandId.random(), new CreateAccount("id2", "username2")))
                        .build())
                .action(builder -> builder
                        .id(action3)
                        .status(ActionStatus.Pending)
                        .command(new ActionCommand<>(CommandId.random(), new CreateAccount("id3", "username3")))
                        .build())
                .build();
        SagaStateTransition transition = new SagaStateTransition.SagaActionStatusChanged(sagaId, action1, ActionStatus.Completed, Collections.emptyList());
        Saga<SpecificRecord> result = SagaUtils.applyTransition(transition, saga);
        assertThat(result.status).isEqualTo(SagaStatus.InProgress);
        assertThat(result.actions.get(action1).status).isEqualTo(ActionStatus.Completed);
    }

    @Test
    void testApplyTransitionSagaActionStatusChangedForCompletedUndo() {
        UUID sagaId = UUID.randomUUID();
        UUID action1 = UUID.randomUUID();
        UUID action2 = UUID.randomUUID();
        UUID action3 = UUID.randomUUID();
        Saga<SpecificRecord> saga = new SagaBuilder<SpecificRecord>()
                .id(sagaId)
                .status(SagaStatus.InFailure)
                .action(builder -> builder
                        .id(action1)
                        .status(ActionStatus.InUndo)
                        .command(new ActionCommand<>(CommandId.random(), new AddFunds("id1", 10.0)))
                        .undoCommand(new ActionCommand<>(CommandId.random(), new AddFunds("id1", -10.0)))
                        .dependency(action3)
                        .build())
                .action(builder -> builder
                        .id(action2)
                        .status(ActionStatus.Failed)
                        .command(new ActionCommand<>(CommandId.random(), new AddFunds("id2", 10.0)))
                        .undoCommand(new ActionCommand<>(CommandId.random(), new AddFunds("id2", -10.0)))
                        .build())
                .action(builder -> builder
                        .id(action3)
                        .status(ActionStatus.InUndo)
                        .command(new ActionCommand<>(CommandId.random(), new AddFunds("id3", 10.0)))
                        .undoCommand(new ActionCommand<>(CommandId.random(), new AddFunds("id3", -10.0)))
                        .build())
                .build();
        SagaStateTransition transition = new SagaStateTransition.SagaActionStatusChanged(sagaId, action1, ActionStatus.Completed, Collections.emptyList());
        Saga<SpecificRecord> result = SagaUtils.applyTransition(transition, saga);
        assertThat(result.status).isEqualTo(SagaStatus.InFailure);
        assertThat(result.actions.get(action1).status).isEqualTo(ActionStatus.Undone);
    }

    @Test
    void testApplyTransitionSagaActionStatusChangedForFailedUndo() {
        UUID sagaId = UUID.randomUUID();
        UUID action1 = UUID.randomUUID();
        UUID action2 = UUID.randomUUID();
        UUID action3 = UUID.randomUUID();
        Saga<SpecificRecord> saga = new SagaBuilder<SpecificRecord>()
                .id(sagaId)
                .status(SagaStatus.InFailure)
                .action(builder -> builder
                        .id(action1)
                        .status(ActionStatus.InUndo)
                        .command(new ActionCommand<>(CommandId.random(), new AddFunds("id1", 10.0)))
                        .undoCommand(new ActionCommand<>(CommandId.random(), new AddFunds("id1", -10.0)))
                        .dependency(action3)
                        .build())
                .action(builder -> builder
                        .id(action2)
                        .status(ActionStatus.Failed)
                        .command(new ActionCommand<>(CommandId.random(), new AddFunds("id2", 10.0)))
                        .undoCommand(new ActionCommand<>(CommandId.random(), new AddFunds("id2", -10.0)))
                        .build())
                .action(builder -> builder
                        .id(action3)
                        .status(ActionStatus.InUndo)
                        .command(new ActionCommand<>(CommandId.random(), new AddFunds("id3", 10.0)))
                        .undoCommand(new ActionCommand<>(CommandId.random(), new AddFunds("id3", -10.0)))
                        .build())
                .build();
        SagaStateTransition transition = new SagaStateTransition.SagaActionStatusChanged(sagaId, action1, ActionStatus.Failed, Collections.emptyList());
        Saga<SpecificRecord> result = SagaUtils.applyTransition(transition, saga);
        assertThat(result.status).isEqualTo(SagaStatus.InFailure);
        assertThat(result.actions.get(action1).status).isEqualTo(ActionStatus.UndoFailed);
    }

    @Test
    void testApplyTransitionSagaStatusChanged() {
        UUID sagaId = UUID.randomUUID();
        UUID action1 = UUID.randomUUID();
        UUID action2 = UUID.randomUUID();
        UUID action3 = UUID.randomUUID();
        Saga<SpecificRecord> saga = new SagaBuilder<SpecificRecord>()
                .id(sagaId)
                .status(SagaStatus.InProgress)
                .action(builder -> builder
                        .id(action1)
                        .status(ActionStatus.Completed)
                        .command(new ActionCommand<>(CommandId.random(), new CreateAccount("id1", "username1")))
                        .build())
                .action(builder -> builder
                        .id(action2)
                        .status(ActionStatus.Completed)
                        .command(new ActionCommand<>(CommandId.random(), new CreateAccount("id2", "username2")))
                        .build())
                .action(builder -> builder
                        .id(action3)
                        .status(ActionStatus.Completed)
                        .command(new ActionCommand<>(CommandId.random(), new CreateAccount("id3", "username3")))
                        .build())
                .build();
        SagaStateTransition transition = new SagaStateTransition.SagaStatusChanged(sagaId, SagaStatus.Completed, Collections.emptyList());
        Saga<SpecificRecord> result = SagaUtils.applyTransition(transition, saga);
        assertThat(result.status).isEqualTo(SagaStatus.Completed);
        assertThat(result.actions).isEqualTo(saga.actions);
    }

    @Test
    void testApplyTransitionList() {
        UUID sagaId = UUID.randomUUID();
        UUID action1 = UUID.randomUUID();
        UUID action2 = UUID.randomUUID();
        UUID action3 = UUID.randomUUID();
        Saga<SpecificRecord> saga = new SagaBuilder<SpecificRecord>()
                .id(sagaId)
                .status(SagaStatus.InProgress)
                .action(builder -> builder
                        .id(action1)
                        .status(ActionStatus.Pending)
                        .command(new ActionCommand<>(CommandId.random(), new CreateAccount("id1", "username1")))
                        .build())
                .action(builder -> builder
                        .id(action2)
                        .status(ActionStatus.Pending)
                        .command(new ActionCommand<>(CommandId.random(), new CreateAccount("id2", "username2")))
                        .build())
                .action(builder -> builder
                        .id(action3)
                        .status(ActionStatus.Pending)
                        .command(new ActionCommand<>(CommandId.random(), new CreateAccount("id3", "username3")))
                        .build())
                .build();
        List<SagaStateTransition.SagaActionStatusChanged> transitions = Stream.of(
                new SagaStateTransition.SagaActionStatusChanged(sagaId, action1, ActionStatus.Completed, Collections.emptyList()),
                new SagaStateTransition.SagaActionStatusChanged(sagaId, action3, ActionStatus.Failed, Collections.emptyList())
        ).collect(Collectors.toList());
        SagaStateTransition transition = new SagaStateTransition.TransitionList(transitions);
        Saga<SpecificRecord> result = SagaUtils.applyTransition(transition, saga);
        assertThat(result.status).isEqualTo(SagaStatus.InProgress);
        assertThat(result.actions.get(action1).status).isEqualTo(ActionStatus.Completed);
        assertThat(result.actions.get(action3).status).isEqualTo(ActionStatus.Failed);
    }

    Saga<SpecificRecord> getTestSaga(ActionStatus... actionStatuses) {
        Map<UUID, SagaAction<SpecificRecord>> sagaActions = new HashMap<>();
        for (ActionStatus s : actionStatuses) {
            sagaActions.put(UUID.randomUUID(), getTestAction(s));
        }
        return Saga.of(UUID.randomUUID(), sagaActions, SagaStatus.InProgress, Sequence.first());
    }

    SagaAction<SpecificRecord> getTestAction(ActionStatus status) {
        return new SagaAction<>(
                UUID.randomUUID(),
                "testAction",
                new ActionCommand<>(CommandId.random(), new CreateAccount("id", "username")),
                Optional.empty(),
                Collections.emptySet(),
                status,
                Collections.emptyList());
    }

    class SagaBuilder<A> {
        private UUID sagaId;
        private SagaStatus status;
        private Map<UUID, SagaAction<A>> actions = new HashMap<>();

        public SagaBuilder<A> id(UUID sagaId) {
            this.sagaId = sagaId;
            return this;
        }

        public SagaBuilder<A> status(SagaStatus status) {
            this.status = status;
            return this;
        }

        public SagaBuilder<A> action(Function1<SagaActionBuilder<A>, SagaAction<A>> actionBuilderConsumer) {
            SagaAction<A> action = actionBuilderConsumer.apply(new SagaActionBuilder<>());
            actions.put(action.actionId, action);
            return this;
        }

        public Saga<A> build() {
            return new Saga<>(sagaId, actions, status, Collections.emptyList(), Sequence.first());
        }
    }

    class SagaActionBuilder<A> {
        private UUID actionId = UUID.randomUUID();
        private String actionType = "testAction";
        private ActionStatus status = ActionStatus.Pending;
        private ActionCommand<A> command;
        private Optional<ActionCommand<A>> undoCommand = Optional.empty();
        private Set<UUID> dependencies = new HashSet<>();
        private List<SagaError> errors = Collections.emptyList();

        public SagaActionBuilder<A> id(UUID actionId) {
            this.actionId = actionId;
            return this;
        }

        public SagaActionBuilder<A> actionType(String actionType) {
            this.actionType = actionType;
            return this;
        }

        public SagaActionBuilder<A> status(ActionStatus status) {
            this.status = status;
            return this;
        }

        public SagaActionBuilder<A> command(ActionCommand<A> command) {
            this.command = command;
            return this;
        }

        public SagaActionBuilder<A> undoCommand(ActionCommand<A> undoCommand) {
            this.undoCommand = Optional.of(undoCommand);
            return this;
        }

        public SagaActionBuilder<A> dependency(UUID dep) {
            this.dependencies.add(dep);
            return this;
        }

        public SagaAction<A> build() {
            return new SagaAction<>(actionId, actionType, command, undoCommand, dependencies, status, errors);
        }
    }
}
