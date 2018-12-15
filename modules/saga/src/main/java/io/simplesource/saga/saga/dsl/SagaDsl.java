package io.simplesource.saga.saga.dsl;

import com.google.common.collect.Lists;
import io.simplesource.data.NonEmptyList;
import io.simplesource.data.Result;
import io.simplesource.data.Sequence;
import io.simplesource.saga.model.saga.*;
import lombok.Value;
import lombok.experimental.ExtensionMethod;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.simplesource.saga.saga.dsl.SagaDsl.*;


public final class SagaDsl {
    @Value
    static final class Fragment<A> {
        List<UUID> input;
        List<UUID> output;
        Optional<SagaBuilder<A>> sagaBuilder;

        Fragment<A> then(Fragment<A> next) {
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

    static <A> Fragment<A> inParallel(Fragment<A>... fragments) {
        return inParallel(Lists.newArrayList(fragments));
    }

    static <A> Fragment<A> inSeries(Fragment<A>... fragments) {
        return inSeries(Lists.newArrayList(fragments));
    }

    static <A> Fragment<A> inParallel(List<Fragment<A>> fragments) {
        Stream<Fragment<A>> fragSteam = fragments.stream();
        Stream<Optional<SagaBuilder<A>>> a = fragSteam.map(x -> x.sagaBuilder);
        Optional<SagaBuilder<A>> c = a.filter(Optional::isPresent).findFirst().flatMap(x -> x);

        return new Fragment<>(
                fragSteam.flatMap(f -> f.input.stream()).collect(Collectors.toList()),
                fragSteam.flatMap(f -> f.output.stream()).collect(Collectors.toList()),
                c);
    }

    static <A> Fragment<A> inSeries(List<Fragment<A>> fragments) {
        Fragment<A> cumulative = new Fragment<>(Collections.emptyList(), Collections.emptyList(), Optional.empty());
        for (Fragment<A> next : fragments) {
            cumulative = cumulative.then(next);
        }

        return cumulative;
    }

    @Value
    static final class SagaBuilder<A> {
        Map<UUID, SagaAction<A>> actions = new HashMap<>();
        Map<UUID, Set<UUID>> dependencies = new HashMap<>();
        List<String> errors = new ArrayList<>();

        Fragment<A> addAction(UUID actionId,
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
                return Result.success(new Saga<>(UUID.randomUUID(), newActions, SagaStatus.NotStarted, Sequence.first()));
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


class X {
    void v() {
        inParallel(Collections.<Fragment<String>>emptyList())
                .then(inParallel(Collections.emptyList()))
                .then(inParallel(Collections.emptyList()));

    }
}


//trait SagaDsl {
//  final case class Fragment<A>(input: List<UUID>, output: List<UUID>, sagaBuilder: Option<SagaBuilder<A>>) {
//    def ~>(next: Fragment<A>): Fragment<A> = {
//      (this.sagaBuilder, next.sagaBuilder) match {
//        case (Some(sb), Some(sbNext)) =>
//          if (sb ne sbNext)
//            sb.errors += s"~> used with actions created by different builders"
//          for {
//            thisId <- this.output
//            nextId <- next.input
//          } sb.dependencies.get(nextId).foreach(nd => sb.dependencies.update(nextId, nd + thisId))
//          Fragment(this.input, next.output, this.sagaBuilder)
//        case (Some(_), None) => this
//        case _               => next
//      }
//    }
//  }
//
//  def inParallel<A>(fragments: Fragment<A>*): Fragment<A> = inParallel(List(fragments: _*))
//
//  def inParallel<A>(fragments: List<Fragment<A>>): Fragment<A> = inParallelImpl(fragments)
//
//  private def inParallelImpl<A>(fragments: List<Fragment<A>>): Fragment<A> =
//    Fragment(fragments.flatMap(_.input),
//             fragments.flatMap(_.output),
//             fragments.map(_.sagaBuilder).find(_.isDefined).flatten)
//
//  def inSeries<A>(fragments: Fragment<A>*): Fragment<A> = inSeriesImpl(List(fragments: _*))
//
//  def inSeries<A>(fragments: List<Fragment<A>>): Fragment<A> = inSeriesImpl(fragments)
//
//  private def inSeriesImpl<A>(fragments: List<Fragment<A>>): Fragment<A> =
//    fragments.foldLeft(Fragment<A>(List.empty, List.empty, None))((cumulative, next) => cumulative ~> next)
//
//  implicit class SBList<A>(sbList: List<Fragment<A>>) {
//    def inSeries(): Fragment<A> = inSeriesImpl(sbList)
//    def parallel(): Fragment<A> = inParallelImpl(sbList)
//  }
//
//  final case class SagaBuilder<A>() {
//    val actions: mutable.HashMap<UUID, SagaAction<A>>  = mutable.HashMap.empty
//    val dependencies: mutable.HashMap<UUID, Set<UUID>> = mutable.HashMap.empty
//    val errors: mutable.ListBuffer<String>             = mutable.ListBuffer.empty
//
//    def addAction(actionId: UUID,
//                  actionType: String,
//                  command: ActionCommand<A>,
//                  undoAction: Option<ActionCommand<A>> = None): Fragment<A> = {
//      val action = SagaAction<A>(actionId = actionId,
//                                 actionType = actionType,
//                                 command = command,
//                                 undoCommand = undoAction,
//                                 dependencies = Set.empty,
//                                 status = ActionStatus.Pending)
//      if (actions.contains(actionId))
//        errors += s"Action Id already used $actionId"
//      actions.update(action.actionId, action)
//      dependencies.update(actionId, Set.empty)
//      Fragment(List(action.actionId), List(action.actionId), Some(this))
//    }
//
//    def build(): Either<SagaError, Saga<A>> = {
//      if (errors.nonEmpty) Left(SagaError(NonEmptyList.fromListUnsafe(errors.toList)))
//      else
//        Right(Saga<A>(actions.mapValues { action =>
//          action.copy(dependencies = dependencies(action.actionId))
//        }.toMap, SagaStatus.NotStarted, 0))
//    }
//  }
//}


//trait SagaDsl {
//  final case class Fragment<A>(input: List<UUID>, output: List<UUID>, sagaBuilder: Option<SagaBuilder<A>>) {
//    def ~>(next: Fragment<A>): Fragment<A> = {
//      (this.sagaBuilder, next.sagaBuilder) match {
//        case (Some(sb), Some(sbNext)) =>
//          if (sb ne sbNext)
//            sb.errors += s"~> used with actions created by different builders"
//          for {
//            thisId <- this.output
//            nextId <- next.input
//          } sb.dependencies.get(nextId).foreach(nd => sb.dependencies.update(nextId, nd + thisId))
//          Fragment(this.input, next.output, this.sagaBuilder)
//        case (Some(_), None) => this
//        case _               => next
//      }
//    }
//  }
//
//  def inParallel<A>(fragments: Fragment<A>*): Fragment<A> = inParallel(List(fragments: _*))
//
//  def inParallel<A>(fragments: List<Fragment<A>>): Fragment<A> = inParallelImpl(fragments)
//
//  private def inParallelImpl<A>(fragments: List<Fragment<A>>): Fragment<A> =
//    Fragment(fragments.flatMap(_.input),
//             fragments.flatMap(_.output),
//             fragments.map(_.sagaBuilder).find(_.isDefined).flatten)
//
//  def inSeries<A>(fragments: Fragment<A>*): Fragment<A> = inSeriesImpl(List(fragments: _*))
//
//  def inSeries<A>(fragments: List<Fragment<A>>): Fragment<A> = inSeriesImpl(fragments)
//
//  private def inSeriesImpl<A>(fragments: List<Fragment<A>>): Fragment<A> =
//    fragments.foldLeft(Fragment<A>(List.empty, List.empty, None))((cumulative, next) => cumulative ~> next)
//
//  implicit class SBList<A>(sbList: List<Fragment<A>>) {
//    def inSeries(): Fragment<A> = inSeriesImpl(sbList)
//    def parallel(): Fragment<A> = inParallelImpl(sbList)
//  }
//
//  final case class SagaBuilder<A>() {
//    val actions: mutable.HashMap<UUID, SagaAction<A>>  = mutable.HashMap.empty
//    val dependencies: mutable.HashMap<UUID, Set<UUID>> = mutable.HashMap.empty
//    val errors: mutable.ListBuffer<String>             = mutable.ListBuffer.empty
//
//    def addAction(actionId: UUID,
//                  actionType: String,
//                  command: ActionCommand<A>,
//                  undoAction: Option<ActionCommand<A>> = None): Fragment<A> = {
//      val action = SagaAction<A>(actionId = actionId,
//                                 actionType = actionType,
//                                 command = command,
//                                 undoCommand = undoAction,
//                                 dependencies = Set.empty,
//                                 status = ActionStatus.Pending)
//      if (actions.contains(actionId))
//        errors += s"Action Id already used $actionId"
//      actions.update(action.actionId, action)
//      dependencies.update(actionId, Set.empty)
//      Fragment(List(action.actionId), List(action.actionId), Some(this))
//    }
//
//    def build(): Either<SagaError, Saga<A>> = {
//      if (errors.nonEmpty) Left(SagaError(NonEmptyList.fromListUnsafe(errors.toList)))
//      else
//        Right(Saga<A>(actions.mapValues { action =>
//          action.copy(dependencies = dependencies(action.actionId))
//        }.toMap, SagaStatus.NotStarted, 0))
//    }
//  }
//}
