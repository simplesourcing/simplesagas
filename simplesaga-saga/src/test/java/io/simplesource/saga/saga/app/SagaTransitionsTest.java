package io.simplesource.saga.saga.app;

import io.simplesource.api.CommandId;
import io.simplesource.data.Sequence;
import io.simplesource.saga.model.action.ActionCommand;
import io.simplesource.saga.model.action.ActionId;
import io.simplesource.saga.model.action.ActionStatus;
import io.simplesource.saga.model.action.SagaAction;
import io.simplesource.saga.model.messages.SagaStateTransition;
import io.simplesource.saga.model.messages.UndoCommand;
import io.simplesource.saga.model.saga.Saga;
import io.simplesource.saga.model.saga.SagaError;
import io.simplesource.saga.model.saga.SagaId;
import io.simplesource.saga.model.saga.SagaStatus;
import io.simplesource.saga.saga.avro.generated.test.AddFunds;
import io.simplesource.saga.saga.avro.generated.test.CreateAccount;
import io.simplesource.saga.saga.avro.generated.test.TransferFunds;
import org.apache.avro.specific.SpecificRecord;
import org.junit.jupiter.api.Test;
import scala.Function1;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

class SagaTransitionsTest {

    @Test
    void testFailedAction() {
        assertThat(ActionStatuses.of(getTestSaga(ActionStatus.Failed)).has(ActionStatus.Failed)).isTrue();
        assertThat(ActionStatuses.of(getTestSaga(ActionStatus.Completed, ActionStatus.Failed)).has(ActionStatus.Failed)).isTrue();
        assertThat(ActionStatuses.of(getTestSaga(ActionStatus.Completed, ActionStatus.InProgress)).has(ActionStatus.Failed)).isFalse();
    }

    @Test
    void testActionInProgress() {
        assertThat(ActionStatuses.of(getTestSaga(ActionStatus.InProgress)).has(ActionStatus.InProgress)).isTrue();
        assertThat(ActionStatuses.of(getTestSaga(ActionStatus.Completed, ActionStatus.InProgress)).has(ActionStatus.InProgress)).isTrue();
        assertThat(ActionStatuses.of(getTestSaga(ActionStatus.Failed)).has(ActionStatus.InProgress)).isFalse();
        assertThat(ActionStatuses.of(getTestSaga(ActionStatus.Completed, ActionStatus.Failed)).has(ActionStatus.InProgress)).isFalse();
    }

    @Test
    void testSagaCompleted() {
        assertThat(ActionStatuses.of(getTestSaga(ActionStatus.Completed)).has(ActionStatus.Completed)).isTrue();
        assertThat(ActionStatuses.of(getTestSaga(ActionStatus.InProgress)).has(ActionStatus.Completed)).isFalse();
        assertThat(ActionStatuses.of(getTestSaga(ActionStatus.InProgress)).missing(ActionStatus.Completed)).isTrue();
        assertThat(ActionStatuses.of(getTestSaga(ActionStatus.Completed, ActionStatus.Failed)).has(ActionStatus.Completed)).isTrue();
    }

    @Test
    void testSagaFailurePending() {
        assertThat(ActionStatuses.of(getTestSaga(ActionStatus.Failed, ActionStatus.InProgress)).failurePending()).isTrue();
        assertThat(ActionStatuses.of(getTestSaga(ActionStatus.Failed, ActionStatus.RetryAwaiting)).failurePending()).isTrue();
        assertThat(ActionStatuses.of(getTestSaga(ActionStatus.InProgress)).failurePending()).isFalse();
        assertThat(ActionStatuses.of(getTestSaga(ActionStatus.Failed, ActionStatus.Completed)).failurePending()).isFalse();
    }

    @Test
    void testSagaInFailure() {
        assertThat(ActionStatuses.of(getTestSaga(ActionStatus.Failed, ActionStatus.InUndo)).inFailure()).isTrue();
        assertThat(ActionStatuses.of(getTestSaga(ActionStatus.Failed, ActionStatus.Completed)).inFailure()).isTrue();
        assertThat(ActionStatuses.of(getTestSaga(ActionStatus.Failed, ActionStatus.InProgress)).inFailure()).isFalse();
        assertThat(ActionStatuses.of(getTestSaga(ActionStatus.InProgress)).inFailure()).isFalse();
    }

    @Test
    void testSagaFailed() {
        assertThat(ActionStatuses.of(getTestSaga(ActionStatus.Failed)).failed()).isTrue();
        assertThat(ActionStatuses.of(getTestSaga(ActionStatus.Failed, ActionStatus.Completed)).failed()).isFalse();
        assertThat(ActionStatuses.of(getTestSaga(ActionStatus.Failed, ActionStatus.InProgress)).failed()).isFalse();
        assertThat(ActionStatuses.of(getTestSaga(ActionStatus.Failed, ActionStatus.InUndo)).failed()).isFalse();
        assertThat(ActionStatuses.of(getTestSaga(ActionStatus.Failed, ActionStatus.UndoBypassed)).failed()).isTrue();
        assertThat(ActionStatuses.of(getTestSaga(ActionStatus.Failed, ActionStatus.Undone)).failed()).isTrue();
        assertThat(ActionStatuses.of(getTestSaga(ActionStatus.Failed, ActionStatus.UndoFailed)).failed()).isTrue();
        assertThat(ActionStatuses.of(getTestSaga(ActionStatus.InProgress)).failed()).isFalse();
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
                        .command(ActionCommand.of(new CreateAccount("id1", "username1"), "action_type"))
                        .build())
                .action(builder -> builder
                        .id(action2)
                        .status(ActionStatus.Completed)
                        .command(ActionCommand.of(new CreateAccount("id2", "username2"), "action_type"))
                        .build())
                .action(builder -> builder
                        .id(action3)
                        .status(ActionStatus.Pending)
                        .command(ActionCommand.of(new CreateAccount("id3", "username3"), "action_type"))
                        .build())
                .build();
        assertThat(ActionResolver.getNextActions(saga).stream().map(a -> a.actionId)).containsExactlyInAnyOrder(action1, action3);
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
                        .command(ActionCommand.of(new CreateAccount("id1", "username1"), "action_type"))
                        .dependency(action3)
                        .build())
                .action(builder -> builder
                        .id(action2)
                        .status(ActionStatus.Completed)
                        .command(ActionCommand.of(new CreateAccount("id2", "username2"), "action_type"))
                        .build())
                .action(builder -> builder
                        .id(action3)
                        .status(ActionStatus.Pending)
                        .command(ActionCommand.of(new CreateAccount("id3", "username3"), "action_type"))
                        .build())
                .build();
        assertThat(ActionResolver.getNextActions(saga).stream().map(a -> a.actionId)).containsOnly(action3);
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
                        .command(ActionCommand.of(new CreateAccount("id1", "username1"), "action_type"))
                        .build())
                .build();
        assertThat(ActionResolver.getNextActions(saga)).isEmpty();
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
                        .command(ActionCommand.of(new CreateAccount("id1", "username1"), "action_type"))
                        .build())
                .action(builder -> builder
                        .id(action2)
                        .status(ActionStatus.Failed)
                        .command(ActionCommand.of(new CreateAccount("id2", "username2"), "action_type"))
                        .build())
                .action(builder -> builder
                        .id(action3)
                        .status(ActionStatus.Completed)
                        .command(ActionCommand.of(new CreateAccount("id3", "username3"), "action_type"))
                        .build())
                .build();
        assertThat(ActionResolver.getNextActions(saga).stream().map(a -> a.actionId)).containsExactlyInAnyOrder(action1, action3);
        assertThat(ActionResolver.getNextActions(saga).stream().map(a -> a.status)).containsOnly(ActionStatus.UndoBypassed);
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
                        .command(ActionCommand.of(new AddFunds("id1", 10.0), "action_type"))
                        .undoCommand(ActionCommand.of(undoCommand1, new AddFunds("id1", -10.0), "action_type"))
                        .build())
                .action(builder -> builder
                        .id(action2)
                        .status(ActionStatus.Failed)
                        .command(ActionCommand.of(new AddFunds("id2", 10.0), "action_type"))
                        .undoCommand(ActionCommand.of(undoCommand2, new AddFunds("id2", -10.0), "action_type"))
                        .build())
                .action(builder -> builder
                        .id(action3)
                        .status(ActionStatus.Completed)
                        .command(ActionCommand.of(new AddFunds("id3", 10.0), "action_type"))
                        .undoCommand(ActionCommand.of(undoCommand3, new AddFunds("id3", -10.0), "action_type"))
                        .build())
                .build();
        assertThat(ActionResolver.getNextActions(saga).stream().map(a -> a.actionId)).containsExactlyInAnyOrder(action1, action3);
        assertThat(ActionResolver.getNextActions(saga).stream().map(a -> a.status)).containsOnly(ActionStatus.InUndo);
        assertThat(ActionResolver.getNextActions(saga).stream().map(a -> a.command.get().commandId)).containsExactlyInAnyOrder(undoCommand1, undoCommand3);
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
                        .command(ActionCommand.of(new AddFunds("id1", 10.0), "action_type"))
                        .undoCommand(ActionCommand.of(undoCommand1, new AddFunds("id1", -10.0), "action_type"))
                        .dependency(action3)
                        .build())
                .action(builder -> builder
                        .id(action2)
                        .status(ActionStatus.Failed)
                        .command(ActionCommand.of(new AddFunds("id2", 10.0), "action_type"))
                        .undoCommand(ActionCommand.of(undoCommand2, new AddFunds("id2", -10.0), "action_type"))
                        .build())
                .action(builder -> builder
                        .id(action3)
                        .status(ActionStatus.Completed)
                        .command(ActionCommand.of(new AddFunds("id3", 10.0), "action_type"))
                        .undoCommand(ActionCommand.of(undoCommand3, new AddFunds("id3", -10.0), "action_type"))
                        .build())
                .build();
        assertThat(ActionResolver.getNextActions(saga).stream().map(a -> a.actionId)).containsOnly(action1);
        assertThat(ActionResolver.getNextActions(saga).stream().map(a -> a.status)).containsOnly(ActionStatus.InUndo);
        assertThat(ActionResolver.getNextActions(saga).stream().map(a -> a.command.get().commandId)).containsOnly(undoCommand1);
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
                        .command(ActionCommand.of(new CreateAccount("id1", "username1"), "action_type"))
                        .build())
                .action(builder -> builder
                        .id(action2)
                        .status(ActionStatus.Completed)
                        .command(ActionCommand.of(new CreateAccount("id2", "username2"), "action_type"))
                        .build())
                .action(builder -> builder
                        .id(action3)
                        .status(ActionStatus.Pending)
                        .command(ActionCommand.of(new CreateAccount("id3", "username3"), "action_type"))
                        .build())
                .build();
        SagaStateTransition<SpecificRecord> transition = SagaStateTransition.SetInitialState.of(saga);
        Saga<SpecificRecord> result = SagaTransition.applyTransition(transition, saga).saga;
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
                        .command(ActionCommand.of(new AddFunds("id1", 10.0), "action_type"))
                        .undoCommand(ActionCommand.of(new AddFunds("id1", -10.0), "action_type"))
                        .build())
                .action(builder -> builder
                        .id(action2)
                        .status(ActionStatus.Completed)
                        .command(ActionCommand.of(new AddFunds("id2", 20.0), "action_type"))
                        .build())
                .action(builder -> builder
                        .id(action3)
                        .status(ActionStatus.Pending)
                        .command(ActionCommand.of(new AddFunds("id3", 30.0), "action_type"))
                        .build())
                .build();
        UndoCommand<SpecificRecord> newUndoCommand = UndoCommand.of(new AddFunds("id1", -11.0), "action_type");
        SagaStateTransition<SpecificRecord> transition = SagaStateTransition.SagaActionStateChanged.of(sagaId,
                action1,
                ActionStatus.Completed,
                Collections.emptyList(),
                Optional.of(newUndoCommand),
                false);
        Saga<SpecificRecord> result = SagaTransition.applyTransition(transition, saga).saga;
        assertThat(result.status).isEqualTo(SagaStatus.InProgress);
        SagaAction<SpecificRecord> newAction1 = result.actions.get(action1);
        assertThat(newAction1.status).isEqualTo(ActionStatus.Completed);
        assertThat(newAction1.undoCommand.map(c -> c.command)).isEqualTo(Optional.of(newUndoCommand.command));
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
                        .command(ActionCommand.of(new AddFunds("id1", 10.0), "action_type"))
                        .undoCommand(ActionCommand.of(new AddFunds("id1", -10.0), "action_type"))
                        .dependency(action3)
                        .build())
                .action(builder -> builder
                        .id(action2)
                        .status(ActionStatus.Failed)
                        .command(ActionCommand.of(new AddFunds("id2", 10.0), "action_type"))
                        .undoCommand(ActionCommand.of(new AddFunds("id2", -10.0), "action_type"))
                        .build())
                .action(builder -> builder
                        .id(action3)
                        .status(ActionStatus.InUndo)
                        .command(ActionCommand.of(new AddFunds("id3", 10.0), "action_type"))
                        .undoCommand(ActionCommand.of(new AddFunds("id3", -10.0), "action_type"))
                        .build())
                .build();
        UndoCommand<SpecificRecord> newUndoCommand = UndoCommand.of(new AddFunds("id1", -11.0), "action_type");
        SagaStateTransition<SpecificRecord> transition = SagaStateTransition.SagaActionStateChanged.of(
                sagaId,
                action1,
                ActionStatus.Undone,
                Collections.emptyList(),
                Optional.of(newUndoCommand),
                true);
        SagaAction<SpecificRecord> oldAction1 = saga.actions.get(action1);
        Saga<SpecificRecord> result = SagaTransition.applyTransition(transition, saga).saga;
        assertThat(result.status).isEqualTo(SagaStatus.InFailure);
        SagaAction<SpecificRecord> newAction1 = result.actions.get(action1);
        assertThat(newAction1.status).isEqualTo(ActionStatus.Undone);
        // passing an Undo action from an undeo execution shouldn't happen,
        // but if it does by action processor implementation error, it has no effect
        assertThat(newAction1.undoCommand).isEqualTo(oldAction1.undoCommand);
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
                        .command(ActionCommand.of(new AddFunds("id1", 10.0), "action_type"))
                        .undoCommand(ActionCommand.of(new AddFunds("id1", -10.0), "action_type"))
                        .dependency(action3)
                        .build())
                .action(builder -> builder
                        .id(action2)
                        .status(ActionStatus.Failed)
                        .command(ActionCommand.of(new AddFunds("id2", 10.0), "action_type"))
                        .undoCommand(ActionCommand.of(new AddFunds("id2", -10.0), "action_type"))
                        .build())
                .action(builder -> builder
                        .id(action3)
                        .status(ActionStatus.InUndo)
                        .command(ActionCommand.of(new AddFunds("id3", 10.0), "action_type"))
                        .undoCommand(ActionCommand.of(new AddFunds("id3", -10.0), "action_type"))
                        .build())
                .build();
        SagaStateTransition<SpecificRecord> transition = SagaStateTransition.SagaActionStateChanged.of(
                sagaId,
                action1,
                ActionStatus.UndoFailed,
                Collections.emptyList(),
                Optional.empty(),
                true);
        Saga<SpecificRecord> result = SagaTransition.applyTransition(transition, saga).saga;
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
                        .command(ActionCommand.of(new CreateAccount("id1", "username1"), "action_type"))
                        .build())
                .action(builder -> builder
                        .id(action2)
                        .status(ActionStatus.Completed)
                        .command(ActionCommand.of(new CreateAccount("id2", "username2"), "action_type"))
                        .undoCommand(ActionCommand.of(new TransferFunds("id2", "id1", 1.0), "action_type"))
                        .build())
                .action(builder -> builder
                        .id(action3)
                        .status(ActionStatus.Completed)
                        .command(ActionCommand.of(new CreateAccount("id3", "username3"), "action_type"))
                        .build())
                .build();
        SagaStateTransition<SpecificRecord> transition =
                SagaStateTransition.SagaStatusChanged.of(
                        sagaId,
                        SagaStatus.Completed,
                        Collections.emptyList());
        Saga<SpecificRecord> result = SagaTransition.applyTransition(transition, saga).saga;
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
                        .command(ActionCommand.of(new CreateAccount("id1", "username1"), "action_type"))
                        .build())
                .action(builder -> builder
                        .id(action2)
                        .status(ActionStatus.Pending)
                        .command(ActionCommand.of(new CreateAccount("id2", "username2"), "action_type"))
                        .build())
                .action(builder -> builder
                        .id(action3)
                        .status(ActionStatus.Pending)
                        .command(ActionCommand.of(new CreateAccount("id3", "username3"), "action_type"))
                        .build())
                .build();
        List<SagaStateTransition.SagaActionStateChanged<SpecificRecord>> transitions = Stream.of(
                SagaStateTransition.SagaActionStateChanged.<SpecificRecord>of(
                        sagaId,
                        action1,
                        ActionStatus.Completed,
                        Collections.emptyList(),
                        Optional.empty(),
                        false),
                SagaStateTransition.SagaActionStateChanged.<SpecificRecord>of(
                        sagaId,
                        action3,
                        ActionStatus.Failed,
                        Collections.emptyList(),
                        Optional.empty(),
                        false)
        ).collect(Collectors.toList());
        SagaStateTransition<SpecificRecord> transition = SagaStateTransition.TransitionList.of(transitions);
        Saga<SpecificRecord> result = SagaTransition.applyTransition(transition, saga).saga;
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
                ActionCommand.of(new CreateAccount("id", "username"), "action_type"),
                Optional.empty(),
                Collections.emptySet(),
                status,
                Collections.emptyList(),
                0);
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
        private ActionStatus status = ActionStatus.Pending;
        private ActionCommand<A> command;
        private Optional<ActionCommand<A>> undoCommand = Optional.empty();
        private Set<ActionId> dependencies = new HashSet<>();
        private List<SagaError> errors = Collections.emptyList();

        public SagaActionBuilder<A> id(ActionId actionId) {
            this.actionId = actionId;
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
            return SagaAction.of(actionId, command, undoCommand, dependencies, status, errors, 0);
        }
    }
}
