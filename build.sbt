import Dependencies._

ThisBuild / scalaVersion := "2.13.6"
ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / organization := "dev.hiro-hori"
ThisBuild / organizationName := "hiro-hori"

ThisBuild / resolvers ++= Seq(
  Resolver.bintrayRepo("digdag", "maven")
)

lazy val root = (project in file("."))
  .settings(
    name := "digdag-operator-snowflake",
    libraryDependencies ++= Seq(
      // ↓ 追加
      "io.digdag" % "digdag-spi" % "0.9.42" % Provided,
      "io.digdag" % "digdag-plugin-utils" % "0.9.42" % Provided
    )
  )

// See https://www.scala-sbt.org/1.x/docs/Using-Sonatype.html for instructions on how to publish to Sonatype.
