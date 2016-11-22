package org.tupol.flink.timeout

import java.util.UUID

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.util.StreamingMultipleProgramsTestBase
import org.junit.Test

import scala.io.Source


class TimeoutDemo3Test extends StreamingMultipleProgramsTestBase {

//  implicit override val patienceConfig = PatienceConfig(timeout = scaled(Span(6000, Millis)))

  @Test
  def test() {

    val outputFile = super.getTempFilePath(UUID.randomUUID().toString)
    val senv = StreamExecutionEnvironment.getExecutionEnvironment

    val inputRecords: Seq[Record] = Seq(Record("a", 1000), Record("a", 1900), Record("b", 2100))
    val inputStream = senv.fromCollection(inputRecords)
    TimeoutDemo3.demoStreamProcessor(inputStream, outputFile)

    senv.execute()

    val result = Source.fromFile(outputFile).getLines.toSeq
    println(s"===========================================")
    result.foreach(x => println(s"########### $x"))

    assert(result.size == 3)
    result.filter(line => line.contains("Success") && (line.contains("1000") || line.contains("1900")))
    result.filter(line => line.contains("Failure") && line.contains("2100") )

  }

}
