package org.tupol.flink.timeout

import java.util.UUID

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.util.StreamingMultipleProgramsTestBase
import org.junit.Test
import org.scalatest.concurrent.Eventually
import org.scalatest.time.{Millis, Span}

import scala.io.Source
import scala.util.Try

class TimeoutDemo4Spec extends StreamingMultipleProgramsTestBase with Eventually {

  implicit override val patienceConfig = PatienceConfig(timeout = scaled(Span(6000, Millis)))

  @Test
  def test() {

    val outputFile = super.getTempFilePath(UUID.randomUUID().toString)
    val senv = StreamExecutionEnvironment.getExecutionEnvironment
    val inputRecords: Seq[Record] = Seq(Record("a", 1900), Record("b", 2100))
    val inputStream = senv.fromCollection(inputRecords)
    TimeoutDemo4.demoStreamProcessor(inputStream, outputFile)

    println(outputFile)

    senv.execute()

    println("------------------------------")

    eventually {
      Try {
        val result = Source.fromFile(outputFile).getLines.toSeq
        result.size
        result.filter(line => line.contains("1900") && line.contains("Success")).size
        result.filter(line => line.contains("2100") && line.contains("Failure")).size
      }
    }
    println("------------------------------")
  }

}
