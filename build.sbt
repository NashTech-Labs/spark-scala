name := "sparkscala"

version := "1.0-SNAPSHOT"

scalaVersion := "2.10.5"

libraryDependencies ++= Seq(
  "org.apache.spark"   %% "spark-core"              % "1.2.1",
  "org.apache.spark"   %% "spark-streaming-twitter" % "1.2.1",
  "org.apache.spark"   %% "spark-sql"               % "1.2.1",
  "org.apache.spark"   %% "spark-mllib"             % "1.2.1"
  )

fork := true
