import sbt.Resolver

scalaVersion := "2.12.7"

name := "simple-sagas-scala"
organization := "io.simplesource"
version := "0.1.0"

val circeV          = "0.10.0"
val simpleSourcingV = "0.2.2"
val simpleSagaV     = "0.2.4-SNAPSHOT"
val kafkaVersion    = "2.0.0"
val catsV           = "1.4.0"

val javaxArtifact = Artifact("javax.ws.rs-api", "jar", "jar")

lazy val baseDeps = Seq(
  libraryDependencies ++= Seq(
    "javax.ws.rs"      % "javax.ws.rs-api"                    % "2.1" artifacts javaxArtifact,
    "org.apache.kafka" %% "kafka"                             % kafkaVersion,
    "io.simplesource"  % "simplesource-command-api"           % simpleSourcingV,
    "io.simplesource"  % "simplesource-command-kafka"         % simpleSourcingV,
    "io.simplesource"  % "simplesource-command-serialization" % simpleSourcingV,
    "io.simplesource"  % "simplesource-command-serialization" % simpleSourcingV,
    "io.simplesource"  % "simplesaga-model"                   % simpleSagaV,
    "io.simplesource"  % "simplesaga-action"                  % simpleSagaV,
    "io.simplesource"  % "simplesaga-http"                    % simpleSagaV,
    "io.simplesource"  % "simplesaga-dsl"                     % simpleSagaV,
    "io.simplesource"  % "simplesaga-saga"                    % simpleSagaV,
  )
)

lazy val scalaDeps = baseDeps ++ Seq(
  libraryDependencies ++= Seq(
    "ch.qos.logback" % "logback-classic" % "1.2.3",
    "org.typelevel"  %% "cats-core"      % catsV,
    "org.scalatest"  %% "scalatest"      % "3.0.5" % Test,
    "org.scalacheck" %% "scalacheck"     % "1.14.0" % Test,
    "io.circe"       %% "circe-core"     % circeV,
    "io.circe"       %% "circe-generic"  % circeV,
    "io.circe"       %% "circe-parser"   % circeV,
    "io.circe"       %% "circe-java8"    % circeV,
  )
)

lazy val userDeps = Seq(
  libraryDependencies ++= Seq(
    "com.lihaoyi" %% "requests" % "0.1.4"
  )
)

resolvers in Global ++= Seq(
  Resolver.sonatypeRepo("releases"),
  Resolver.sonatypeRepo("snapshots"),
  Resolver.mavenLocal,
  "confluent" at "http://packages.confluent.io/maven/"
)

scalacOptions in ThisBuild := Seq(
  // following two lines must be "together"
  "-encoding",
  "UTF-8",
  "-Xlint",
  "-Xlint:missing-interpolator",
  //"-Xlog-implicits", // enable when trying to debug implicits
  "-deprecation",
  "-feature",
  "-unchecked",
  "-Ywarn-dead-code",
  "-Yno-adapted-args",
  "-language:existentials",
  "-language:higherKinds",
  "-language:implicitConversions",
  "-Ywarn-value-discard",
  // "-Ywarn-unused-import", // seems to be broken for some imports [2.11]
  //"-Ypartial-unification", // enable once we go scala 2.12, fixes si-2712
  // "-Ywarn-unused", // broken in frontned [2.11]
  "-Ywarn-numeric-widen"
)

// ---- kind projector to have cleaner type lambdas ----
val kindProjectorPlugin = Seq(
  addCompilerPlugin("org.spire-math" %% "kind-projector" % "0.9.7")
)

val commonSettings = kindProjectorPlugin

lazy val scala =
  Project(id = "scala", base = file("modules/scala"))
    .settings(commonSettings, scalaDeps)

lazy val user =
  Project(id = "user", base = file("modules/user"))
    .settings(commonSettings, scalaDeps, userDeps)
    .dependsOn(scala)
