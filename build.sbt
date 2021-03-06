name := "flink-study"

version := "0.1.0"

scalaVersion := "2.10.4"


val flinkVersion = "1.2.0"

lazy val providedDependencies = Seq(
  "org.apache.flink" %% "flink-scala" % flinkVersion force(),
  "org.apache.flink" %% "flink-clients" % flinkVersion force(),
  "org.apache.flink" %% "flink-streaming-scala" % flinkVersion force()
)

libraryDependencies ++= providedDependencies //.map(_ % "provided")


libraryDependencies ++= Seq(
  "org.scalacheck" %% "scalacheck" % "1.12.5" % "test",
  "org.scalatest" %% "scalatest" % "2.2.6" % "test",
  "junit" % "junit" % "4.12" % "test",
  "org.apache.flink" %% "flink-test-utils" % flinkVersion % "test" force()
)

// ------------------------------
// TESTING
parallelExecution in Test := false

fork in Test := true
