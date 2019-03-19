package io.simplesource.saga.dsl;

import io.simplesource.data.NonEmptyList;
import io.simplesource.data.Result;
import io.simplesource.data.Sequence;
import io.simplesource.saga.model.action.ActionCommand;
import io.simplesource.saga.model.action.ActionStatus;
import io.simplesource.saga.model.action.SagaAction;
import io.simplesource.saga.model.saga.*;
import io.simplesource.saga.shared.utils.Lists;
import lombok.Value;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public final class SagaDsl {
    @Value
    public static final class SubSaga<A> {
        List<UUID> input;
        List<UUID> output;
        Optional<SagaBuilder<A>> sagaBuilder;

        public SubSaga<A> andThen(SubSaga<A> next) {
            Optional<SagaBuilder<A>> sbO = this.sagaBuilder;
            Optional<SagaBuilder<A>> sbNextO = next.sagaBuilder;

            if (sbO.isPresent() && sbNextO.isPresent()) {
                SagaBuilder<A> sb = sbO.get();
                SagaBuilder<A> sbNext = sbNextO.get();
                if (sb != sbNext)
                    sb.errors.add("Actions created by different builders");
                for (UUID thisId : this.output) {
                    for (UUID nextId : next.input) {
                        Set<UUID> e = sb.dependencies.get(nextId);
                        if (e != null) {
                            // TODO: need decent immutable collections
                            Set<UUID> newSet = new HashSet<>(e);
                            newSet.add(thisId);
                            sb.dependencies.put(nextId, newSet);
                        }
                    }
                }
                return new SubSaga<>(this.input, next.output, this.sagaBuilder);
            } else if (sbO.isPresent()) {
                return this;
            }
            return next;
        }
    }

    public static <A> SubSaga<A> inParallel(SubSaga<A>... subSagas) {

        return inParallel(Lists.of(subSagas));
    }

    public static <A> SubSaga<A> inSeries(SubSaga<A>... subSagas) {
        return inSeries(Lists.of(subSagas));
    }

    public static <A> SubSaga<A> inParallel(Collection<SubSaga<A>> subSagas) {
        Stream<Optional<SagaBuilder<A>>> a = subSagas.stream().map(x -> x.sagaBuilder);
        Optional<SagaBuilder<A>> c = a.filter(Optional::isPresent).findFirst().flatMap(x -> x);

        return new SubSaga<>(
                subSagas.stream().flatMap(f -> f.input.stream()).collect(Collectors.toList()),
                subSagas.stream().flatMap(f -> f.output.stream()).collect(Collectors.toList()),
                c);
    }

    public static <A> SubSaga<A> inSeries(Iterable<SubSaga<A>> subSagas) {
        SubSaga<A> cumulative = new SubSaga<>(Collections.emptyList(), Collections.emptyList(), Optional.empty());
        for (SubSaga<A> next : subSagas) {
            cumulative = cumulative.andThen(next);
        }

        return cumulative;
    }

    @Value(staticConstructor = "create")
    public static final class SagaBuilder<A> {
        private Map<UUID, SagaAction<A>> actions = new HashMap<>();
        private Map<UUID, Set<UUID>> dependencies = new HashMap<>();
        private List<String> errors = new ArrayList<>();

        private SubSaga<A> addAction(UUID actionId,
                                     String actionType,
                                     ActionCommand<A> actionCommand,
                                     Optional<ActionCommand<A>> undoAction) {
            SagaAction<A> action = new SagaAction<>(actionId,
                    actionType,
                    actionCommand,
                    undoAction,
                    Collections.emptySet(),
                    ActionStatus.Pending,
                    Collections.emptyList());

            if (actions.containsKey(actionId))
                errors.add(String.format("Action Id already used %s", actionId));
            actions.put(action.actionId, action);
            dependencies.put(actionId, Collections.emptySet());
            List<UUID> actionIdList = Collections.singletonList(action.actionId);
            return new SubSaga<>(actionIdList, actionIdList, Optional.of(this));
        }

        public SubSaga<A> addAction(UUID actionId,
                                    String actionType,
                                    ActionCommand<A> actionCommand) {
            return addAction(actionId, actionType, actionCommand, Optional.empty());
        }

        public SubSaga<A> addAction(UUID actionId,
                                    String actionType,
                                    ActionCommand<A> actionCommand,
                                    ActionCommand<A> undoActionCommand) {
            return addAction(actionId, actionType, actionCommand, Optional.of(undoActionCommand));
        }

        public Result<SagaError, Saga<A>> build() {
            if (errors.isEmpty()) {
                Map<UUID, SagaAction<A>> newActions = actions.entrySet().stream().map(entry -> {
                    SagaAction<A> eAct = entry.getValue();
                    return new SagaAction<>(eAct.actionId,
                            eAct.actionType,
                            eAct.command,
                            eAct.undoCommand,
                            dependencies.get(entry.getKey()),
                            eAct.status,
                            Collections.emptyList());
                }).collect(Collectors.toMap(sa -> sa.actionId, sa -> sa));
                return Result.success(Saga.of(UUID.randomUUID(), newActions, SagaStatus.NotStarted, Sequence.first()));
            } else {
                NonEmptyList<SagaError> nelError = NonEmptyList.fromList(
                        errors.stream()
                                .map(e -> SagaError.of(SagaError.Reason.InternalError, e))
                                .collect(Collectors.toList()))
                        .get();
                return Result.failure(nelError);
            }
        }
    }
}
