import Dependencies._

ThisBuild / scalaVersion     := "2.13.6"
//ThisBuild / scalaVersion     := "2.11.8"    // For Hortomworks HDP.
ThisBuild / version          := "0.1.0-SNAPSHOT"
ThisBuild / organization     := "com.example"
ThisBuild / organizationName := "example"

lazy val root = (project in file("."))
  .settings(
    name := "sparksqltest",
    //libraryDependencies += scalaTest % Test
    libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.8" % Test
    // ,libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.3"
    ,libraryDependencies += "org.apache.spark" %% "spark-core" % "3.3.0"
    // ,libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.3"
    ,libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.3.0"
  )

// See https://www.scala-sbt.org/1.x/docs/Using-Sonatype.html for instructions on how to publish to Sonatype.
