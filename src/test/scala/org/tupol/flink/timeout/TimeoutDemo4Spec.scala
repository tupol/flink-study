package org.tupol.flink.timeout

import java.util.UUID

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.util.StreamingMultipleProgramsTestBase
import org.junit.Test

import scala.io.Source

class TimeoutDemo4Spec extends StreamingMultipleProgramsTestBase {

  @Test
  def test() {

    val outputFile = super.getTempFilePath(UUID.randomUUID().toString)
    val senv = StreamExecutionEnvironment.getExecutionEnvironment
    val inputRecords: Seq[Record] = Seq(Record("a", 1900), Record("b", 2100))
    val inputStream = senv.fromCollection(inputRecords)
    TimeoutDemo4.demoStreamProcessor(inputStream, outputFile)

    senv.execute()

    val result = Source.fromFile(outputFile).getLines.toSeq
    result.size
    result.filter(line => line.contains("1900") && line.contains("Success")).size
    result.filter(line => line.contains("2100") && line.contains("Failure")).size

  }

}
