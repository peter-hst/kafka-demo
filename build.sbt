ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "3.2.1"

val kafkaVersion = "3.3.1"

lazy val root = (project in file("."))
  .settings(
    name := "kafka-demo",
    idePackagePrefix := Some("hst.peter.kafka")
  )

libraryDependencies ++= Seq(

  // log
  "org.slf4j" % "slf4j-api" % "2.0.5",
  "ch.qos.logback" % "logback-classic" % "1.4.5",

  // kafka
  "org.apache.kafka" % "kafka-clients" % kafkaVersion,
  "org.apache.kafka" % "kafka-streams" % kafkaVersion
)
scalacOptions ++= Seq(
  "-deprecation",
  "-explain",
  "-explain-types",
  "-new-syntax",
  "-unchecked",
  "-Xfatal-warnings",
  "-Xmigration"
)
