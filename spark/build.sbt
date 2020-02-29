name          := "private-set-union"
organization  := "com.microsoft"
description   := "Differentially Private Set Union"
version       := "0.1.0"
scalaVersion  := "2.12.10"
scalacOptions := Seq("-deprecation", "-unchecked", "-encoding", "utf8", "-Xlint")
parallelExecution in Test := false

val sparkVersion        = "2.4.5"
val scalaTestVersion    = "3.1.1"
val scalaCheckVersion   = "1.14.1"

libraryDependencies ++= Seq(
  "org.apache.spark"  %% "spark-core"      % sparkVersion,

  "org.scalatest"     %% "scalatest"       % scalaTestVersion  % "test",
  "org.scalacheck"    %% "scalacheck"      % scalaCheckVersion % "test")


