package io.simplesource.saga.saga.dsl;

import com.google.common.collect.Lists;
import io.simplesource.data.NonEmptyList;
import io.simplesource.data.Result;
import io.simplesource.data.Sequence;
import io.simplesource.saga.model.saga.*;
import lombok.Value;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public final class SagaDsl {
    @Value
    public static final class Fragment<A> {
        List<UUID> input;
        List<UUID> output;
        Optional<SagaBuilder<A>> sagaBuilder;

        public Fragment<A> andThen(Fragment<A> next) {
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
                return new Fragment<>(this.input, next.output, this.sagaBuilder);
            } else if (sbO.isPresent()) {
                return this;
            }
            return next;
        }
    }

    public static <A> Fragment<A> inParallel(Fragment<A>... fragments) {

        return inParallel(Lists.newArrayList(fragments));
    }

    public static <A> Fragment<A> inSeries(Fragment<A>... fragments) {
        return inSeries(Lists.newArrayList(fragments));
    }

    public static <A> Fragment<A> inParallel(Collection<Fragment<A>> fragments) {
        Stream<Optional<SagaBuilder<A>>> a = fragments.stream().map(x -> x.sagaBuilder);
        Optional<SagaBuilder<A>> c = a.filter(Optional::isPresent).findFirst().flatMap(x -> x);

        return new Fragment<>(
                fragments.stream().flatMap(f -> f.input.stream()).collect(Collectors.toList()),
                fragments.stream().flatMap(f -> f.output.stream()).collect(Collectors.toList()),
                c);
    }

    public static <A> Fragment<A> inSeries(Iterable<Fragment<A>> fragments) {
        Fragment<A> cumulative = new Fragment<>(Collections.emptyList(), Collections.emptyList(), Optional.empty());
        for (Fragment<A> next : fragments) {
            cumulative = cumulative.andThen(next);
        }

        return cumulative;
    }

    @Value(staticConstructor = "create")
    public static final class SagaBuilder<A> {
        Map<UUID, SagaAction<A>> actions = new HashMap<>();
        Map<UUID, Set<UUID>> dependencies = new HashMap<>();
        List<String> errors = new ArrayList<>();

        private Fragment<A> addAction(UUID actionId,
                                      String actionType,
                                      ActionCommand<A> actionCommand,
                                      Optional<ActionCommand<A>> undoAction) {
            SagaAction<A> action = new SagaAction<A>(actionId,
                    actionType,
                    actionCommand,
                    undoAction,
                    Collections.emptySet(),
                    ActionStatus.Pending,
                    Optional.empty());

            if (actions.containsKey(actionId))
                errors.add(String.format("Action Id already used %s", actionId));
            actions.put(action.actionId, action);
            dependencies.put(actionId, Collections.emptySet());
            List<UUID> actionIdList = Collections.singletonList(action.actionId);
            return new Fragment<>(actionIdList, actionIdList, Optional.of(this));
        }

        public Fragment<A> addAction(UUID actionId,
                                     String actionType,
                                     ActionCommand<A> actionCommand) {
            return addAction(actionId, actionType, actionCommand, Optional.empty());
        }

        public Fragment<A> addAction(UUID actionId,
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
                            Optional.empty());
                }).collect(Collectors.toMap(sa -> sa.actionId, sa -> sa));
                return Result.success(Saga.of(UUID.randomUUID(), newActions, SagaStatus.NotStarted, Sequence.first()));
            } else {
                NonEmptyList<SagaError> nelError = NonEmptyList.fromList(
                        errors.stream().map(e -> SagaError.of(SagaError.Reason.InternalError, e))
                                .collect(Collectors.toList()))
                        .get();
                return Result.failure(nelError);
            }
        }
    }
}
