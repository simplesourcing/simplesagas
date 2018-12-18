package io.simplesource.saga.saga.dsl

import io.simplesource.saga.saga.dsl.SagaDsl.Fragment

import scala.collection.JavaConverters._

object SagaScalaDsl {
  implicit class FragmentOps[A](fragment: Fragment[A]) {
    def ~>(next: Fragment[A]): Fragment[A] =
      fragment.andThen(next)
  }

  def inParallel[A](fragments: Fragment[A]*): Fragment[A] = inParallel(List(fragments: _*))

  def inParallel[A](fragments: List[Fragment[A]]): Fragment[A] = inParallelImpl(fragments)

  private def inParallelImpl[A](fragments: List[Fragment[A]]): Fragment[A] =
    SagaDsl.inParallel(fragments.asJava)

  def inSeries[A](fragments: Fragment[A]*): Fragment[A] = inSeriesImpl(List(fragments: _*))

  def inSeries[A](fragments: List[Fragment[A]]): Fragment[A] = inSeriesImpl(fragments)

  private def inSeriesImpl[A](fragments: List[Fragment[A]]): Fragment[A] =
    SagaDsl.inSeries(fragments.asJava)

  implicit class SBList[A](sbList: List[Fragment[A]]) {
    def inSeries(): Fragment[A] = inSeriesImpl(sbList)
    def parallel(): Fragment[A] = inParallelImpl(sbList)
  }
}
