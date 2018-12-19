import sbt.Resolver
// The simplest possible sbt build file is just one line:

scalaVersion := "2.12.7"

name := "simple-sagas"
organization := "io.simplesource"
version := "0.1.0"

val circeV          = "0.10.0"
val avro4sV         = "2.0.0-M1"
val simpleSourcingV = "0.2.2-SNAPSHOT"
val kafkaVersion    = "2.0.0"
val catsV           = "1.4.0"

val javaxArtifact = Artifact("javax.ws.rs-api", "jar", "jar")

lazy val modelDeps = Seq(
  libraryDependencies ++= Seq(
    "javax.ws.rs"      % "javax.ws.rs-api"                    % "2.1" artifacts javaxArtifact,
    "org.apache.kafka" %% "kafka"                             % kafkaVersion,
    "io.simplesource"  % "simplesource-command-api"           % simpleSourcingV,
    "io.simplesource"  % "simplesource-command-kafka"         % simpleSourcingV,
    "io.simplesource"  % "simplesource-command-serialization" % simpleSourcingV
  )
)

lazy val coreDeps = modelDeps ++ Seq(
  libraryDependencies ++= Seq(
    "javax.ws.rs"        % "javax.ws.rs-api"          % "2.1" artifacts javaxArtifact,
    "ch.qos.logback"     % "logback-classic"          % "1.2.3",
    "org.apache.kafka"   % "kafka-streams-test-utils" % kafkaVersion % Test,
    "org.junit.jupiter"  % "junit-jupiter-api"        % "5.2.0" % Test,
    "org.junit.jupiter"  % "junit-jupiter-engine"     % "5.2.0" % Test,
    "org.junit.platform" % "junit-platform-engine"    % "1.2.0" % Test,
    "org.assertj"        % "assertj-core-java8"       % "1.0.0m1" % Test
  )
)

lazy val scalaDeps = modelDeps ++ Seq(
  libraryDependencies ++= Seq(
    "ch.qos.logback"      % "logback-classic" % "1.2.3",
    "org.typelevel"       %% "cats-core"      % catsV,
    "com.sksamuel.avro4s" %% "avro4s-core"    % avro4sV,
    "org.scalatest"       %% "scalatest"      % "3.0.5" % Test,
    "org.scalacheck"      %% "scalacheck"     % "1.14.0" % Test,
    "io.circe"            %% "circe-core"     % circeV,
    "io.circe"            %% "circe-generic"  % circeV,
    "io.circe"            %% "circe-parser"   % circeV,
    "io.circe"            %% "circe-java8"    % circeV,
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

lazy val sharedDeps = shared % "compile->compile;test->test"

val commonSettings = kindProjectorPlugin

lazy val model =
  Project(id = "model", base = file("modules/model"))
    .settings(commonSettings, modelDeps)

lazy val shared =
  Project(id = "shared", base = file("modules/shared"))
    .settings(commonSettings, coreDeps)
    .dependsOn(model)

lazy val action =
  Project(id = "action", base = file("modules/action"))
    .settings(commonSettings, coreDeps)
    .dependsOn(model, sharedDeps)

lazy val http =
  Project(id = "http", base = file("modules/http"))
    .settings(commonSettings, coreDeps)
    .dependsOn(model, sharedDeps, action)

lazy val saga =
  Project(id = "saga", base = file("modules/saga"))
    .settings(commonSettings, coreDeps)
    .dependsOn(model, sharedDeps)

lazy val scala =
  Project(id = "scala", base = file("modules/scala"))
    .settings(commonSettings, scalaDeps)
    .dependsOn(model, sharedDeps, saga)

lazy val user =
  Project(id = "user", base = file("modules/user"))
    .settings(commonSettings, coreDeps, scalaDeps, userDeps)
    .dependsOn(model, sharedDeps, action, http, saga, scala)
