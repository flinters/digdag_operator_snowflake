import Dependencies._

ThisBuild / scalaVersion := "2.13.18"
ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / organization := "dev.hiro-hori"
ThisBuild / organizationName := "hiro-hori"

lazy val root = (project in file("."))
  .settings(
    name := "digdag-operator-snowflake",
    libraryDependencies ++= Seq(
      scalaTest % Test,
      "io.digdag" % "digdag-spi" % "0.10.5.1" % Provided,
      "io.digdag" % "digdag-plugin-utils" % "0.10.5.1" % Provided,
      "net.snowflake" % "snowflake-jdbc" % "4.3.2",
      "org.bouncycastle" % "bcpkix-jdk18on" % "1.84",
    )
  )

// See https://www.scala-sbt.org/1.x/docs/Using-Sonatype.html for instructions on how to publish to Sonatype.