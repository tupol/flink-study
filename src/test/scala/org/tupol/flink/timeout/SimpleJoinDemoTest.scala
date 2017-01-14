package org.tupol.flink.timeout

import java.io.File

import org.apache.commons.io.FileUtils
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.util.StreamingProgramTestBase
import org.apache.flink.test.util.TestBaseUtils
import org.junit.Test
import org.scalatest.Matchers
import org.tupol.flink.timeout.demo.{Record, SimpleJoinDemo}
import org.tupol.flink.timeout.demo.RecordTimestampExtractor

import scala.collection.mutable.ArrayBuffer
import scala.util.Try

/**
 * Simple test for TimeoutSimpleDemo1 using the Flink test infrastructure.
 */
class SimpleJoinDemoTest extends StreamingProgramTestBase with Matchers {

  @Test
  def testProgram(): Unit = {

    val outputFile: String = File.createTempFile("result-path", "dir").toURI().toString()

    Try {
      val senv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
      senv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

      val inputRecords: Seq[Record] = Seq(Record("a", 3000, 10), Record("b", 2500, 20), Record("c", 1500, 30), Record("d", 1000, 40))
      val inputStream = senv.fromCollection(inputRecords).assignTimestampsAndWatermarks(RecordTimestampExtractor)

      SimpleJoinDemo.demoStreamProcessor(inputStream, outputFile)

      senv.execute()

      import scala.collection.JavaConversions._

      val results = ArrayBuffer[String]()

      TestBaseUtils.readAllResultLines(results, outputFile)

      // Test that the order of the events is preserved
      results.zip(inputRecords).forall{ case(a, e) => a.contains(e)} shouldBe true
      // Test that the short events succeeded
      results.filter(line => line.contains("Success") && (line.contains("1000") || line.contains("1500"))).size shouldBe 2
      // Test that the dragging events failed
      results.filter(line => line.contains("Failure") && (line.contains("2500") || line.contains("3000"))).size shouldBe 2
    }

    Try { FileUtils.deleteDirectory(new File(outputFile)) }

  }

}
