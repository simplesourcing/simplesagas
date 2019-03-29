package io.simplesource.saga.saga.app;

import io.simplesource.api.CommandId;
import io.simplesource.data.Sequence;
import io.simplesource.saga.model.action.ActionCommand;
import io.simplesource.saga.model.action.ActionId;
import io.simplesource.saga.model.action.ActionStatus;
import io.simplesource.saga.model.action.SagaAction;
import io.simplesource.saga.model.messages.SagaStateTransition;
import io.simplesource.saga.model.saga.Saga;
import io.simplesource.saga.model.saga.SagaError;
import io.simplesource.saga.model.saga.SagaId;
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
        ActionId action1 = ActionId.random();
        ActionId action2 = ActionId.random();
        ActionId action3 = ActionId.random();
        Saga<SpecificRecord> saga = new SagaBuilder<SpecificRecord>()
                .id(SagaId.random())
                .status(SagaStatus.InProgress)
                .action(builder -> builder
                        .id(action1)
                        .status(ActionStatus.Pending)
                        .command(ActionCommand.of(CommandId.random(), new CreateAccount("id1", "username1")))
                        .build())
                .action(builder -> builder
                        .id(action2)
                        .status(ActionStatus.Completed)
                        .command(ActionCommand.of(CommandId.random(), new CreateAccount("id2", "username2")))
                        .build())
                .action(builder -> builder
                        .id(action3)
                        .status(ActionStatus.Pending)
                        .command(ActionCommand.of(CommandId.random(), new CreateAccount("id3", "username3")))
                        .build())
                .build();
        assertThat(SagaUtils.getNextActions(saga).stream().map(a -> a.actionId)).containsExactlyInAnyOrder(action1, action3);
    }

    @Test
    void testNextActionsHandlesDependencies() {
        ActionId action1 = ActionId.random();
        ActionId action2 = ActionId.random();
        ActionId action3 = ActionId.random();
        Saga<SpecificRecord> saga = new SagaBuilder<SpecificRecord>()
                .id(SagaId.random())
                .status(SagaStatus.InProgress)
                .action(builder -> builder
                        .id(action1)
                        .status(ActionStatus.Pending)
                        .command(ActionCommand.of(CommandId.random(), new CreateAccount("id1", "username1")))
                        .dependency(action3)
                        .build())
                .action(builder -> builder
                        .id(action2)
                        .status(ActionStatus.Completed)
                        .command(ActionCommand.of(CommandId.random(), new CreateAccount("id2", "username2")))
                        .build())
                .action(builder -> builder
                        .id(action3)
                        .status(ActionStatus.Pending)
                        .command(ActionCommand.of(CommandId.random(), new CreateAccount("id3", "username3")))
                        .build())
                .build();
        assertThat(SagaUtils.getNextActions(saga).stream().map(a -> a.actionId)).containsOnly(action3);
    }

    @Test
    void testNextActionsReturnsNothingIfCompleted() {
        ActionId action1 = ActionId.random();
        Saga<SpecificRecord> saga = new SagaBuilder<SpecificRecord>()
                .id(SagaId.random())
                .status(SagaStatus.Completed)
                .action(builder -> builder
                        .id(action1)
                        .status(ActionStatus.Pending)
                        .command(ActionCommand.of(CommandId.random(), new CreateAccount("id1", "username1")))
                        .build())
                .build();
        assertThat(SagaUtils.getNextActions(saga)).isEmpty();
    }

    @Test
    void testNextActionsReturnsUndoBypassedActions() {
        ActionId action1 = ActionId.random();
        ActionId action2 = ActionId.random();
        ActionId action3 = ActionId.random();
        Saga<SpecificRecord> saga = new SagaBuilder<SpecificRecord>()
                .id(SagaId.random())
                .status(SagaStatus.InFailure)
                .action(builder -> builder
                        .id(action1)
                        .status(ActionStatus.Completed)
                        .command(ActionCommand.of(CommandId.random(), new CreateAccount("id1", "username1")))
                        .build())
                .action(builder -> builder
                        .id(action2)
                        .status(ActionStatus.Failed)
                        .command(ActionCommand.of(CommandId.random(), new CreateAccount("id2", "username2")))
                        .build())
                .action(builder -> builder
                        .id(action3)
                        .status(ActionStatus.Completed)
                        .command(ActionCommand.of(CommandId.random(), new CreateAccount("id3", "username3")))
                        .build())
                .build();
        assertThat(SagaUtils.getNextActions(saga).stream().map(a -> a.actionId)).containsExactlyInAnyOrder(action1, action3);
        assertThat(SagaUtils.getNextActions(saga).stream().map(a -> a.status)).containsOnly(ActionStatus.UndoBypassed);
    }

    @Test
    void testNextActionsReturnsUndoActions() {
        ActionId action1 = ActionId.random();
        ActionId action2 = ActionId.random();
        ActionId action3 = ActionId.random();
        CommandId undoCommand1 = CommandId.random();
        CommandId undoCommand2 = CommandId.random();
        CommandId undoCommand3 = CommandId.random();
        Saga<SpecificRecord> saga = new SagaBuilder<SpecificRecord>()
                .id(SagaId.random())
                .status(SagaStatus.InFailure)
                .action(builder -> builder
                        .id(action1)
                        .status(ActionStatus.Completed)
                        .command(ActionCommand.of(CommandId.random(), new AddFunds("id1", 10.0)))
                        .undoCommand(ActionCommand.of(undoCommand1, new AddFunds("id1", -10.0)))
                        .build())
                .action(builder -> builder
                        .id(action2)
                        .status(ActionStatus.Failed)
                        .command(ActionCommand.of(CommandId.random(), new AddFunds("id2", 10.0)))
                        .undoCommand(ActionCommand.of(undoCommand2, new AddFunds("id2", -10.0)))
                        .build())
                .action(builder -> builder
                        .id(action3)
                        .status(ActionStatus.Completed)
                        .command(ActionCommand.of(CommandId.random(), new AddFunds("id3", 10.0)))
                        .undoCommand(ActionCommand.of(undoCommand3, new AddFunds("id3", -10.0)))
                        .build())
                .build();
        assertThat(SagaUtils.getNextActions(saga).stream().map(a -> a.actionId)).containsExactlyInAnyOrder(action1, action3);
        assertThat(SagaUtils.getNextActions(saga).stream().map(a -> a.status)).containsOnly(ActionStatus.InUndo);
        assertThat(SagaUtils.getNextActions(saga).stream().map(a -> a.command.get().commandId)).containsExactlyInAnyOrder(undoCommand1, undoCommand3);
    }

    @Test
    void testNextActionsHandlesDependenciesForUndo() {
        ActionId action1 = ActionId.random();
        ActionId action2 = ActionId.random();
        ActionId action3 = ActionId.random();
        CommandId undoCommand1 = CommandId.random();
        CommandId undoCommand2 = CommandId.random();
        CommandId undoCommand3 = CommandId.random();
        Saga<SpecificRecord> saga = new SagaBuilder<SpecificRecord>()
                .id(SagaId.random())
                .status(SagaStatus.InFailure)
                .action(builder -> builder
                        .id(action1)
                        .status(ActionStatus.Completed)
                        .command(ActionCommand.of(CommandId.random(), new AddFunds("id1", 10.0)))
                        .undoCommand(ActionCommand.of(undoCommand1, new AddFunds("id1", -10.0)))
                        .dependency(action3)
                        .build())
                .action(builder -> builder
                        .id(action2)
                        .status(ActionStatus.Failed)
                        .command(ActionCommand.of(CommandId.random(), new AddFunds("id2", 10.0)))
                        .undoCommand(ActionCommand.of(undoCommand2, new AddFunds("id2", -10.0)))
                        .build())
                .action(builder -> builder
                        .id(action3)
                        .status(ActionStatus.Completed)
                        .command(ActionCommand.of(CommandId.random(), new AddFunds("id3", 10.0)))
                        .undoCommand(ActionCommand.of(undoCommand3, new AddFunds("id3", -10.0)))
                        .build())
                .build();
        assertThat(SagaUtils.getNextActions(saga).stream().map(a -> a.actionId)).containsOnly(action1);
        assertThat(SagaUtils.getNextActions(saga).stream().map(a -> a.status)).containsOnly(ActionStatus.InUndo);
        assertThat(SagaUtils.getNextActions(saga).stream().map(a -> a.command.get().commandId)).containsOnly(undoCommand1);
    }

    @Test
    void testApplyTransitionInitialState() {
        ActionId action1 = ActionId.random();
        ActionId action2 = ActionId.random();
        ActionId action3 = ActionId.random();
        Saga<SpecificRecord> saga = new SagaBuilder<SpecificRecord>()
                .id(SagaId.random())
                .status(SagaStatus.NotStarted)
                .action(builder -> builder
                        .id(action1)
                        .status(ActionStatus.Pending)
                        .command(ActionCommand.of(CommandId.random(), new CreateAccount("id1", "username1")))
                        .build())
                .action(builder -> builder
                        .id(action2)
                        .status(ActionStatus.Completed)
                        .command(ActionCommand.of(CommandId.random(), new CreateAccount("id2", "username2")))
                        .build())
                .action(builder -> builder
                        .id(action3)
                        .status(ActionStatus.Pending)
                        .command(ActionCommand.of(CommandId.random(), new CreateAccount("id3", "username3")))
                        .build())
                .build();
        SagaStateTransition transition = SagaStateTransition.SetInitialState.of(saga);
        Saga<SpecificRecord> result = SagaUtils.applyTransition(transition, saga);
        assertThat(result.status).isEqualTo(SagaStatus.InProgress);
        assertThat(result.actions).isEqualTo(saga.actions);
    }

    @Test
    void testApplyTransitionSagaActionStatusChanged() {
        SagaId sagaId = SagaId.random();
        ActionId action1 = ActionId.random();
        ActionId action2 = ActionId.random();
        ActionId action3 = ActionId.random();
        Saga<SpecificRecord> saga = new SagaBuilder<SpecificRecord>()
                .id(sagaId)
                .status(SagaStatus.InProgress)
                .action(builder -> builder
                        .id(action1)
                        .status(ActionStatus.Pending)
                        .command(ActionCommand.of(CommandId.random(), new CreateAccount("id1", "username1")))
                        .build())
                .action(builder -> builder
                        .id(action2)
                        .status(ActionStatus.Completed)
                        .command(ActionCommand.of(CommandId.random(), new CreateAccount("id2", "username2")))
                        .build())
                .action(builder -> builder
                        .id(action3)
                        .status(ActionStatus.Pending)
                        .command(ActionCommand.of(CommandId.random(), new CreateAccount("id3", "username3")))
                        .build())
                .build();
        SagaStateTransition transition = SagaStateTransition.SagaActionStatusChanged.of(sagaId, action1, ActionStatus.Completed, Collections.emptyList());
        Saga<SpecificRecord> result = SagaUtils.applyTransition(transition, saga);
        assertThat(result.status).isEqualTo(SagaStatus.InProgress);
        assertThat(result.actions.get(action1).status).isEqualTo(ActionStatus.Completed);
    }

    @Test
    void testApplyTransitionSagaActionStatusChangedForCompletedUndo() {
        SagaId sagaId = SagaId.random();
        ActionId action1 = ActionId.random();
        ActionId action2 = ActionId.random();
        ActionId action3 = ActionId.random();
        Saga<SpecificRecord> saga = new SagaBuilder<SpecificRecord>()
                .id(sagaId)
                .status(SagaStatus.InFailure)
                .action(builder -> builder
                        .id(action1)
                        .status(ActionStatus.InUndo)
                        .command(ActionCommand.of(CommandId.random(), new AddFunds("id1", 10.0)))
                        .undoCommand(ActionCommand.of(CommandId.random(), new AddFunds("id1", -10.0)))
                        .dependency(action3)
                        .build())
                .action(builder -> builder
                        .id(action2)
                        .status(ActionStatus.Failed)
                        .command(ActionCommand.of(CommandId.random(), new AddFunds("id2", 10.0)))
                        .undoCommand(ActionCommand.of(CommandId.random(), new AddFunds("id2", -10.0)))
                        .build())
                .action(builder -> builder
                        .id(action3)
                        .status(ActionStatus.InUndo)
                        .command(ActionCommand.of(CommandId.random(), new AddFunds("id3", 10.0)))
                        .undoCommand(ActionCommand.of(CommandId.random(), new AddFunds("id3", -10.0)))
                        .build())
                .build();
        SagaStateTransition transition = SagaStateTransition.SagaActionStatusChanged.of(sagaId, action1, ActionStatus.Completed, Collections.emptyList());
        Saga<SpecificRecord> result = SagaUtils.applyTransition(transition, saga);
        assertThat(result.status).isEqualTo(SagaStatus.InFailure);
        assertThat(result.actions.get(action1).status).isEqualTo(ActionStatus.Undone);
    }

    @Test
    void testApplyTransitionSagaActionStatusChangedForFailedUndo() {
        SagaId sagaId = SagaId.random();
        ActionId action1 = ActionId.random();
        ActionId action2 = ActionId.random();
        ActionId action3 = ActionId.random();
        Saga<SpecificRecord> saga = new SagaBuilder<SpecificRecord>()
                .id(sagaId)
                .status(SagaStatus.InFailure)
                .action(builder -> builder
                        .id(action1)
                        .status(ActionStatus.InUndo)
                        .command(ActionCommand.of(CommandId.random(), new AddFunds("id1", 10.0)))
                        .undoCommand(ActionCommand.of(CommandId.random(), new AddFunds("id1", -10.0)))
                        .dependency(action3)
                        .build())
                .action(builder -> builder
                        .id(action2)
                        .status(ActionStatus.Failed)
                        .command(ActionCommand.of(CommandId.random(), new AddFunds("id2", 10.0)))
                        .undoCommand(ActionCommand.of(CommandId.random(), new AddFunds("id2", -10.0)))
                        .build())
                .action(builder -> builder
                        .id(action3)
                        .status(ActionStatus.InUndo)
                        .command(ActionCommand.of(CommandId.random(), new AddFunds("id3", 10.0)))
                        .undoCommand(ActionCommand.of(CommandId.random(), new AddFunds("id3", -10.0)))
                        .build())
                .build();
        SagaStateTransition transition = SagaStateTransition.SagaActionStatusChanged.of(sagaId, action1, ActionStatus.Failed, Collections.emptyList());
        Saga<SpecificRecord> result = SagaUtils.applyTransition(transition, saga);
        assertThat(result.status).isEqualTo(SagaStatus.InFailure);
        assertThat(result.actions.get(action1).status).isEqualTo(ActionStatus.UndoFailed);
    }

    @Test
    void testApplyTransitionSagaStatusChanged() {
        SagaId sagaId = SagaId.random();
        ActionId action1 = ActionId.random();
        ActionId action2 = ActionId.random();
        ActionId action3 = ActionId.random();
        Saga<SpecificRecord> saga = new SagaBuilder<SpecificRecord>()
                .id(sagaId)
                .status(SagaStatus.InProgress)
                .action(builder -> builder
                        .id(action1)
                        .status(ActionStatus.Completed)
                        .command(ActionCommand.of(CommandId.random(), new CreateAccount("id1", "username1")))
                        .build())
                .action(builder -> builder
                        .id(action2)
                        .status(ActionStatus.Completed)
                        .command(ActionCommand.of(CommandId.random(), new CreateAccount("id2", "username2")))
                        .build())
                .action(builder -> builder
                        .id(action3)
                        .status(ActionStatus.Completed)
                        .command(ActionCommand.of(CommandId.random(), new CreateAccount("id3", "username3")))
                        .build())
                .build();
        SagaStateTransition transition = SagaStateTransition.SagaStatusChanged.of(sagaId, SagaStatus.Completed, Collections.emptyList());
        Saga<SpecificRecord> result = SagaUtils.applyTransition(transition, saga);
        assertThat(result.status).isEqualTo(SagaStatus.Completed);
        assertThat(result.actions).isEqualTo(saga.actions);
    }

    @Test
    void testApplyTransitionList() {
        SagaId sagaId = SagaId.random();
        ActionId action1 = ActionId.random();
        ActionId action2 = ActionId.random();
        ActionId action3 = ActionId.random();
        Saga<SpecificRecord> saga = new SagaBuilder<SpecificRecord>()
                .id(sagaId)
                .status(SagaStatus.InProgress)
                .action(builder -> builder
                        .id(action1)
                        .status(ActionStatus.Pending)
                        .command(ActionCommand.of(CommandId.random(), new CreateAccount("id1", "username1")))
                        .build())
                .action(builder -> builder
                        .id(action2)
                        .status(ActionStatus.Pending)
                        .command(ActionCommand.of(CommandId.random(), new CreateAccount("id2", "username2")))
                        .build())
                .action(builder -> builder
                        .id(action3)
                        .status(ActionStatus.Pending)
                        .command(ActionCommand.of(CommandId.random(), new CreateAccount("id3", "username3")))
                        .build())
                .build();
        List<SagaStateTransition.SagaActionStatusChanged> transitions = Stream.of(
                SagaStateTransition.SagaActionStatusChanged.of(sagaId, action1, ActionStatus.Completed, Collections.emptyList()),
                SagaStateTransition.SagaActionStatusChanged.of(sagaId, action3, ActionStatus.Failed, Collections.emptyList())
        ).collect(Collectors.toList());
        SagaStateTransition transition = SagaStateTransition.TransitionList.of(transitions);
        Saga<SpecificRecord> result = SagaUtils.applyTransition(transition, saga);
        assertThat(result.status).isEqualTo(SagaStatus.InProgress);
        assertThat(result.actions.get(action1).status).isEqualTo(ActionStatus.Completed);
        assertThat(result.actions.get(action3).status).isEqualTo(ActionStatus.Failed);
    }

    Saga<SpecificRecord> getTestSaga(ActionStatus... actionStatuses) {
        Map<ActionId, SagaAction<SpecificRecord>> sagaActions = new HashMap<>();
        for (ActionStatus s : actionStatuses) {
            sagaActions.put(ActionId.random(), getTestAction(s));
        }
        return Saga.of(SagaId.random(), sagaActions, SagaStatus.InProgress, Sequence.first());
    }

    SagaAction<SpecificRecord> getTestAction(ActionStatus status) {
        return SagaAction.of(
                ActionId.random(),
                "testAction",
                ActionCommand.of(CommandId.random(), new CreateAccount("id", "username")),
                Optional.empty(),
                Collections.emptySet(),
                status,
                Collections.emptyList());
    }

    class SagaBuilder<A> {
        private SagaId sagaId;
        private SagaStatus status;
        private Map<ActionId, SagaAction<A>> actions = new HashMap<>();

        public SagaBuilder<A> id(SagaId sagaId) {
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
        private ActionId actionId = ActionId.random();
        private String actionType = "testAction";
        private ActionStatus status = ActionStatus.Pending;
        private ActionCommand<A> command;
        private Optional<ActionCommand<A>> undoCommand = Optional.empty();
        private Set<ActionId> dependencies = new HashSet<>();
        private List<SagaError> errors = Collections.emptyList();

        public SagaActionBuilder<A> id(ActionId actionId) {
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

        public SagaActionBuilder<A> dependency(ActionId dep) {
            this.dependencies.add(dep);
            return this;
        }

        public SagaAction<A> build() {
            return SagaAction.of(actionId, actionType, command, undoCommand, dependencies, status, errors);
        }
    }
}
