ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / scalaVersion := "2.12.13"

lazy val root = (project in file("."))
  .settings(
    name := "SparkStreamingApp",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % "3.5.0",
      "org.apache.spark" %% "spark-sql" % "3.5.0",
      // Ajouter la dépendance pour Spark Streaming
      "org.apache.spark" %% "spark-streaming" % "3.5.0"
    )
  )
