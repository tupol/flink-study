name := "flink-study"

version := "0.1.0"

scalaVersion := "2.10.4"


val flinkVersion = "1.1.3"

// ------------------------------
// DEPENDENCIES AND RESOLVERS

lazy val providedDependencies = Seq(
  "org.apache.flink" %% "flink-scala" % flinkVersion force(),
  "org.apache.flink" %% "flink-clients" % flinkVersion force(),
  "org.apache.flink" %% "flink-streaming-scala" % flinkVersion force()
)

libraryDependencies ++= providedDependencies.map(_ % "provided")
