package org.tupol.flink.timeout

import java.io.File

import org.apache.commons.io.FileUtils
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.util.StreamingProgramTestBase
import org.apache.flink.test.util.TestBaseUtils
import org.junit.Test
import org.tupol.flink.timeout.utils.RecordTimestampExtractor

import scala.collection.mutable.ArrayBuffer
import scala.util.Try


class TimeoutDemo5Test extends StreamingProgramTestBase {

  @Test
  def testProgram(): Unit = {

    val outputFile: String = File.createTempFile("result-path", "dir").toURI().toString()

    Try {
      val senv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
      senv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

      val inputRecords: Seq[Record] = Seq(Record("a", 1000), Record("b", 1500), Record("c", 2500), Record("c", 3000))
      val inputStream = senv.fromCollection(inputRecords).assignTimestampsAndWatermarks(RecordTimestampExtractor)

      TimeoutDemo3.demoStreamProcessor(inputStream, outputFile)

      senv.execute()

      import scala.collection.JavaConversions._

      val result = ArrayBuffer[String]()

      TestBaseUtils.readAllResultLines(result, outputFile)

      println("------------------------------")
      result.foreach(println)
      println("------------------------------")

    }

    Try { FileUtils.deleteDirectory(new File(outputFile)) }

  }

}
