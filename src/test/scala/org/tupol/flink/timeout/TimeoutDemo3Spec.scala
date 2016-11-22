package org.tupol.flink.timeout

import java.io.File
import java.util.UUID

import org.apache.flink.streaming.api.scala._
import org.scalatest.concurrent.Eventually
import org.scalatest.time.{Millis, Span}
import org.scalatest.{FunSuite, Matchers}
import org.tupol.flink.utils.TestDirectorySpec

import scala.io.Source

class TimeoutDemo3Spec extends FunSuite with Matchers with Eventually with TestDirectorySpec {

  implicit override val patienceConfig = PatienceConfig(timeout = scaled(Span(4000, Millis)))

  test("Simple test for TimeoutDemo3") {

    val senv = StreamExecutionEnvironment.createLocalEnvironment(4)
    val inputRecords: Seq[Record] = Seq(Record("a", 1000), Record("a", 1900), Record("b", 2100))
    val inputStream = senv.fromCollection(inputRecords)
    val outputFile = new File(testDir, UUID.randomUUID().toString).getAbsolutePath
    TimeoutDemo3.demoStreamProcessor(inputStream, outputFile)

    senv.execute()

    eventually{
      val result = Source.fromFile(outputFile).getLines.toSeq
      result.size shouldBe 3
      result.filter(line => line.contains("Success") && (line.contains("1000") || line.contains("1900"))).size shouldBe 2
      result.filter(line => line.contains("Failure") && line.contains("2100") ).size shouldBe 1
    }

  }

}
