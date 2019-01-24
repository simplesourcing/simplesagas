package io.simplesource.saga.scala.dsl

import io.simplesource.saga.saga.dsl.SagaDsl
import io.simplesource.saga.saga.dsl.SagaDsl.SubSaga

import scala.collection.JavaConverters._

trait SagaScalaDsl {
  implicit class SubSagaOps[A](subSaga: SubSaga[A]) {
    def ~>(next: SubSaga[A]): SubSaga[A] =
      subSaga.andThen(next)
  }

  def inParallel[A](subSagas: SubSaga[A]*): SubSaga[A] =
    inParallel(List(subSagas: _*))

  def inParallel[A](subSagas: List[SubSaga[A]]): SubSaga[A] =
    inParallelImpl(subSagas)

  private def inParallelImpl[A](subSagas: List[SubSaga[A]]): SubSaga[A] =
    SagaDsl.inParallel(subSagas.asJava)

  def inSeries[A](subSagas: SubSaga[A]*): SubSaga[A] =
    inSeriesImpl(List(subSagas: _*))

  def inSeries[A](subSagas: List[SubSaga[A]]): SubSaga[A] =
    inSeriesImpl(subSagas)

  private def inSeriesImpl[A](subSagas: List[SubSaga[A]]): SubSaga[A] =
    SagaDsl.inSeries(subSagas.asJava)

  implicit class SBList[A](sbList: List[SubSaga[A]]) {
    def inSeries(): SubSaga[A] = inSeriesImpl(sbList)
    def parallel(): SubSaga[A] = inParallelImpl(sbList)
  }
}
