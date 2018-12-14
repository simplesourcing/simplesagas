package saga.dsl

import java.util.UUID

import cats.data.NonEmptyList
import model.saga._

import scala.collection.mutable

trait SagaDsl {
  final case class Fragment[A](input: List[UUID], output: List[UUID], sagaBuilder: Option[SagaBuilder[A]]) {
    def ~>(next: Fragment[A]): Fragment[A] = {
      (this.sagaBuilder, next.sagaBuilder) match {
        case (Some(sb), Some(sbNext)) =>
          if (sb ne sbNext)
            sb.errors += s"~> used with actions created by different builders"
          for {
            thisId <- this.output
            nextId <- next.input
          } sb.dependencies.get(nextId).foreach((nd: Set[UUID]) => sb.dependencies.update(nextId, nd + thisId))
          Fragment(this.input, next.output, this.sagaBuilder)
        case (Some(_), None) => this
        case _               => next
      }
    }
  }

  def inParallel[A](fragments: Fragment[A]*): Fragment[A] = inParallel(List(fragments: _*))

  def inParallel[A](fragments: List[Fragment[A]]): Fragment[A] = inParallelImpl(fragments)

  private def inParallelImpl[A](fragments: List[Fragment[A]]): Fragment[A] =
    Fragment(fragments.flatMap(_.input),
             fragments.flatMap(_.output),
             fragments.map(_.sagaBuilder).find(_.isDefined).flatten)

  def inSeries[A](fragments: Fragment[A]*): Fragment[A] = inSeriesImpl(List(fragments: _*))

  def inSeries[A](fragments: List[Fragment[A]]): Fragment[A] = inSeriesImpl(fragments)

  private def inSeriesImpl[A](fragments: List[Fragment[A]]): Fragment[A] =
    fragments.foldLeft(Fragment[A](List.empty, List.empty, None))((cumulative, next) => cumulative ~> next)

  implicit class SBList[A](sbList: List[Fragment[A]]) {
    def inSeries(): Fragment[A] = inSeriesImpl(sbList)
    def parallel(): Fragment[A] = inParallelImpl(sbList)
  }

  final case class SagaBuilder[A]() {
    val actions: mutable.HashMap[UUID, SagaAction[A]]  = mutable.HashMap.empty
    val dependencies: mutable.HashMap[UUID, Set[UUID]] = mutable.HashMap.empty
    val errors: mutable.ListBuffer[String]             = mutable.ListBuffer.empty

    def addAction(actionId: UUID,
                  actionType: String,
                  command: ActionCommand[A],
                  undoAction: Option[ActionCommand[A]] = None): Fragment[A] = {
      val action = SagaAction[A](actionId = actionId,
                                 actionType = actionType,
                                 command = command,
                                 undoCommand = undoAction,
                                 dependencies = Set.empty,
                                 status = ActionStatus.Pending)
      if (actions.contains(actionId))
        errors += s"Action Id already used $actionId"
      actions.update(action.actionId, action)
      dependencies.update(actionId, Set.empty)
      Fragment(List(action.actionId), List(action.actionId), Some(this))
    }

    def build(): Either[SagaError, Saga[A]] = {
      if (errors.nonEmpty) Left(SagaError(NonEmptyList.fromListUnsafe(errors.toList)))
      else
        Right(Saga[A](actions.mapValues { action =>
          action.copy(dependencies = dependencies(action.actionId))
        }.toMap, SagaStatus.NotStarted, 0))
    }
  }
}
