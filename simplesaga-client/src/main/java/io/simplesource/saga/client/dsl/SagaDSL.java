package io.simplesource.saga.client.dsl;

import io.simplesource.data.NonEmptyList;
import io.simplesource.data.Result;
import io.simplesource.saga.model.action.ActionCommand;
import io.simplesource.saga.model.action.ActionId;
import io.simplesource.saga.model.action.ActionStatus;
import io.simplesource.saga.model.action.SagaAction;
import io.simplesource.saga.model.saga.*;
import lombok.Value;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * The Saga DSL is used be the client to build a saga.
 * <p>
 * To build a saga:
 * <ol>
 *     <li>Create a {@link SagaBuilder SagaBuilder} with {@link SagaDSL#createBuilder()}</li>
 *     <li>Use the {@link SagaBuilder SagaBuilder} to add saga actions. Each {@link SagaBuilder#addAction(ActionId, String, Object, Object) addAction()} creates a {@link SubSaga SubSaga}.</li>
 *     <li>Define the dependencies between the saga actions with {@link SubSaga#andThen(SubSaga) andThen()}.
 *     Defining dependencies between actions creates subsagas consisting of multiple actions. Dependencies can be defined between these subsagas.</li>
 *     <li>Create the saga with {@link SagaBuilder#build() build()}. Any {@link SubSaga SubSaga}s that are not dependent on each other can be executed in parallel.</li>
 * </ol>
 * <p>
 * For example:
 * <pre>{@code
 *     import io.simplesource.saga.client.dsl.SagaDSL;
 *     import static io.simplesource.saga.client.dsl.SagaDSL;
 *
 *     SagaBuilder<A> sagaBuilder = SagaDSL.createBuilder();
 *
 *     SubSaga<A> actionA = sagaBuilder.addAction(actionIdA, actionCommandA, undoCommandA);
 *     SubSaga<A> actionB = sagaBuilder.addAction(actionIdB, actionB, undoActionB);
 *     SubSaga<A> actionC = sagaBuilder.addAction(actionIdC, actionC, undoActionC);
 *
 *     inParallel(actionA, actionB).andThen(actionC);
 *
 *     Result<SagaError, Saga<A>> sagaBuildResult = sagaBuilder.build();
 * }</pre>
 * <p>
 * The {@link SubSaga#andThen(SubSaga) andThen} operator is associative. This means that the following are equivalent:
 * <p>
 * <pre>{@code     a.andThen(b.andThen(c))}</pre>
 * <p>
 * and
 * <p>
 * <pre>{@code     a.andThen(b).andThen(c)}</pre>
 */
public final class SagaDSL {
    /**
     * A {@code Subsaga} is a subset of a saga consisting of one or more action, with dependencies defined between them.
     * <p>
     * When adding an action with {@link SagaBuilder#addAction(ActionId, ActionCommand, Optional) SagaBuilder.addAction()} a SubSaga is created with a single action.
     * <p>
     * Defining dependencies with {@link SubSaga#andThen(SubSaga) andThen()} has the effect of combining two {@code Subsaga}s into a combined {@code Subsaga} with the action from both of them in the prescribed order.
     *
     * @param <A> a representation of an action command that is shared across all actions in the saga. This is typically a generic type, such as Json, or if using Avro serialization, SpecificRecord or GenericRecord
     */
    @Value
    public static final class SubSaga<A> {
        private final List<ActionId> input;
        private final List<ActionId> output;
        private final Optional<SagaBuilder<A>> sagaBuilder;

        /**
         * Defines a dependency between two saga actions (or two sub-sagas).
         *
         * @param next the actions of {@code next} subsaga are only executed once the actions of {@code this} has completed.
         * @return the subsaga comprised of {@code this} and {@code next} combined
         */
        public SubSaga<A> andThen(SubSaga<A> next) {
            Optional<SagaBuilder<A>> sbO = this.sagaBuilder;
            Optional<SagaBuilder<A>> sbNextO = next.sagaBuilder;

            if (sbO.isPresent() && sbNextO.isPresent()) {
                SagaBuilder<A> sb = sbO.get();
                SagaBuilder<A> sbNext = sbNextO.get();
                if (sb != sbNext)
                    sb.errors.add("Actions created by different builders");
                for (ActionId thisId : this.output) {
                    for (ActionId nextId : next.input) {
                        Set<ActionId> e = sb.dependencies.get(nextId);
                        if (e != null) {
                            Set<ActionId> newSet = new HashSet<>(e);
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

    /**
     * Creates a {@code SagaBuilder}
     *
     * @param <A> a representation of an action command that is shared across all actions in the saga. This is typically a generic type, such as Json, or if using Avro serialization, SpecificRecord or GenericRecord
     * @return the saga builder
     */
    public static <A> SagaBuilder<A> createBuilder() {
        return SagaBuilder.create();
    }

    /**
     * Creates a subsagas consisting of one or more subsagas executing in parallel. In parallel means they have the same dependencies, the same subsagas depend on them, but they are not dependent on each other.
     *
     * @param <A> a representation of an action command that is shared across all actions in the saga. This is typically a generic type, such as Json, or if using Avro serialization, SpecificRecord or GenericRecord
     * @param subSagas a sub saga
     * @param subSagas the sub sagas
     * @return a sub saga consisting of all the input subsagas executed in parallel
     */
    public static <A> SubSaga<A> inParallel(SubSaga<A> subSaga, SubSaga<A>... subSagas) {
        return inParallel(asList(subSaga, subSagas));
    }

    /**
     * Creates a subsagas consisting of one or more subsagas executing in series.
     *
     * @param <A> a representation of an action command that is shared across all actions in the saga. This is typically a generic type, such as Json, or if using Avro serialization, SpecificRecord or GenericRecord
     * @param subSagas the sub sagas
     * @return a sub saga consisting of all the input subsagas executed in series
     */
    public static <A> SubSaga<A> inSeries(SubSaga<A> subSaga, SubSaga<A>... subSagas) {
        return inSeries(asList(subSaga, subSagas));
    }

    /**
     * Creates a subsagas consisting of a collection of subsagas executing in parallel. In parallel means they have the same dependencies, the same subsagas depend on them, but they are not dependent on each other.
     *
     * @param <A> a representation of an action command that is shared across all actions in the saga. This is typically a generic type, such as Json, or if using Avro serialization, SpecificRecord or GenericRecord
     * @param subSagas A collection of subsagas
     * @return a sub saga consisting of all the input subsagas executed in parallel
     */
    public static <A> SubSaga<A> inParallel(Collection<SubSaga<A>> subSagas) {
        Stream<Optional<SagaBuilder<A>>> a = subSagas.stream().map(x -> x.sagaBuilder);
        Optional<SagaBuilder<A>> c = a.filter(Optional::isPresent).findFirst().flatMap(x -> x);

        return new SubSaga<>(
                subSagas.stream().flatMap(f -> f.input.stream()).collect(Collectors.toList()),
                subSagas.stream().flatMap(f -> f.output.stream()).collect(Collectors.toList()),
                c);
    }

    /**
     * Creates a subsagas consisting of an ordered collection of subsagas executing in series.
     *
     * @param <A> a representation of an action command that is shared across all actions in the saga. This is typically a generic type, such as Json, or if using Avro serialization, SpecificRecord or GenericRecord
     * @param subSagas A collection of subsagas
     * @return a sub saga consisting of all the input subsagas executed in series
     */
    public static <A> SubSaga<A> inSeries(Iterable<SubSaga<A>> subSagas) {
        SubSaga<A> cumulative = new SubSaga<>(Collections.emptyList(), Collections.emptyList(), Optional.empty());
        for (SubSaga<A> next : subSagas) {
            cumulative = cumulative.andThen(next);
        }

        return cumulative;
    }

    private static <A> List<SubSaga<A>> asList(SubSaga<A> subSaga, SubSaga<A>[] subSagas) {
        List<SubSaga<A>> l = new ArrayList<>();
        l.add(subSaga);
        l.addAll(Arrays.asList(subSagas));
        return l;
    }

    /**
     * A SagaBuilder is used to add actions by creating a subsaga consisting of a single action, and to build the saga once the definition steps are complete.
     *
     * @param <A> a representation of an action command that is shared across all actions in the saga. This is typically a generic type, such as Json, or if using Avro serialization, SpecificRecord or GenericRecord
     */
    @Value(staticConstructor = "create")
    public static final class SagaBuilder<A> {
        private Map<ActionId, SagaAction<A>> actions = new HashMap<>();
        private Map<ActionId, Set<ActionId>> dependencies = new HashMap<>();
        private List<String> errors = new ArrayList<>();

        private SubSaga<A> addAction(ActionId actionId,
                                     ActionCommand<A> actionCommand,
                                     Optional<ActionCommand<A>> undoAction) {
            SagaAction<A> action = SagaAction.of(actionId,
                    actionCommand,
                    undoAction,
                    Collections.emptySet(),
                    ActionStatus.Pending,
                    Collections.emptyList(),
                    0);

            if (actions.containsKey(actionId))
                errors.add(String.format("Action Id already used %s", actionId));
            actions.put(action.actionId, action);
            dependencies.put(actionId, Collections.emptySet());
            List<ActionId> actionIdList = Collections.singletonList(action.actionId);
            return new SubSaga<>(actionIdList, actionIdList, Optional.of(this));
        }

        /**
         * Adds an action with a random Id and no undo action command, creating a sub saga with a single action.
         *
         * @param actionType    the action type
         * @param actionCommand the action command
         * @return the sub saga
         */
        public SubSaga<A> addAction(String actionType,
                                    A actionCommand) {
            return addAction(ActionId.random(), ActionCommand.of(actionCommand, actionType), Optional.empty());
        }

        /**
         * Adds an action with a random Id and an undo action command, creating a sub saga with a single action.
         *
         * @param actionType        the action type
         * @param actionCommand     the action command
         * @param undoActionCommand the undo action command
         * @return the sub saga
         */
        public SubSaga<A> addAction(String actionType,
                                    A actionCommand,
                                    A undoActionCommand) {
            return addAction(ActionId.random(), ActionCommand.of(actionCommand, actionType), Optional.of(ActionCommand.of(undoActionCommand, actionType)));
        }

        /**
         * Adds an action with no undo action command, creating a sub saga with a single action.
         *
         * @param actionId      the action id
         * @param actionType    the action type
         * @param actionCommand the action command
         * @return the sub saga
         */
        public SubSaga<A> addAction(ActionId actionId,
                                    String actionType,
                                    A actionCommand) {
            return addAction(actionId, ActionCommand.of(actionCommand, actionType), Optional.empty());
        }

        /**
         * Adds an action with an undo action command, creating a sub saga with a single action.
         *
         * @param actionId          the action id
         * @param actionType        the action type
         * @param actionCommand     the action command
         * @param undoActionCommand the undo action command
         * @return the sub saga
         */
        public SubSaga<A> addAction(ActionId actionId,
                                    String actionType,
                                    A actionCommand,
                                    A undoActionCommand) {
            return addAction(actionId, ActionCommand.of(actionCommand, actionType), Optional.of(ActionCommand.of(undoActionCommand, actionType)));
        }

        /**
         * builds the Saga from all the subsagas that were previously defined by creating single actions with
         * {@link SagaBuilder#addAction(String, Object, Object) addAction} and by defining the dependencies between these actions.
         * <p>
         * The {@link Saga} returned can be considered an immutable data structure. Any mutations to saga state from this point on will result in a new instance being created.
         *
         * @return the result, either successful with the built {@link Saga}, or a {@link SagaError}.
         */
        public Result<SagaError, Saga<A>> build() {
            if (errors.isEmpty()) {
                Map<ActionId, SagaAction<A>> newActions = actions.entrySet().stream().map(entry -> {
                    SagaAction<A> eAct = entry.getValue();
                    return SagaAction.of(eAct.actionId,
                            eAct.command,
                            eAct.undoCommand,
                            dependencies.get(entry.getKey()),
                            eAct.status,
                            Collections.emptyList(),
                            eAct.retryCount);
                }).collect(Collectors.toMap(sa -> sa.actionId, sa -> sa));
                return Result.success(Saga.of(newActions));
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
