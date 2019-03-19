package io.simplesource.saga.scala.dsl
import java.util.UUID

import io.simplesource.saga.model.action.ActionCommand
import io.simplesource.saga.model.saga.Saga
import io.simplesource.saga.dsl.SagaDsl.SagaBuilder
import org.scalatest.{Matchers, WordSpec}

import scala.collection.JavaConverters._

class ScalaDslTest extends WordSpec with Matchers {

  implicit class DependsOps(action: String) {
    def shouldDependOnSet[A](dependsOn: Set[String])(implicit sagaState: Saga[A]) = {
      sagaState.actions.asScala.values
        .find(_.actionType == s"actionType-$action")
        .get
        .dependencies
        .asScala
        .map(sagaState.actions.get(_))
        .map(_.actionType) shouldBe dependsOn.map(a => s"actionType-$a")
    }

    def shouldDependOn[A](dependsOn: String)(implicit sagaState: Saga[A]) =
      shouldDependOnSet(Set(dependsOn))
  }

  "action dsl" must {
    "create action dependency graph" in {
      def rId(): UUID = UUID.randomUUID()

      val builder = SagaBuilder.create[String]

      def create(a: String) =
        builder.addAction(rId(), s"actionType-$a", new ActionCommand(rId(), s"Command-$a"))

      val a1 = create("1")
      val a2 = create("2")
      val a3 = create("3")

      val a4  = create("4")
      val a5a = create("5a")
      val a5b = create("5b")
      val a6  = create("6")

      val a7  = create("7")
      val a8a = create("8a")
      val a8b = create("8b")
      val a9  = create("9")

      val a10  = create("10")
      val a11a = create("11a")
      val a11b = create("11b")
      val a12  = create("12")

      val a13 = create("13")

      a1 ~> a2 ~> a3

      a4 ~> inParallel(a5a, a5b) ~> a6

      a7 ~> inSeries(a8a, a8b) ~> a9

      (a10 ~> a11a) ~> a12
      a10 ~> (a11b ~> a12)

      a7 ~> (a13 ~> a9)

      implicit val sagaState: Saga[String] = builder.build().getOrElse(null)

      "1" shouldDependOnSet Set.empty
      "2" shouldDependOn "1"
      "3" shouldDependOn "2"

      "4" shouldDependOnSet Set.empty
      "5a" shouldDependOn "4"
      "5b" shouldDependOn "4"

      "6" shouldDependOnSet Set("5a", "5b")

      "7" shouldDependOnSet Set.empty
      "8a" shouldDependOn "7"
      "8b" shouldDependOn "8a"
      "9" shouldDependOnSet Set("8b", "13")

      "10" shouldDependOnSet Set.empty
      "11a" shouldDependOn "10"
      "11b" shouldDependOn "10"
      "12" shouldDependOnSet Set("11a", "11b")

      "13" shouldDependOn "7"
    }
  }
}
