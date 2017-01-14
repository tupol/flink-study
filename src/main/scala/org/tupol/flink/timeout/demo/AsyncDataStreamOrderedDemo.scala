package org.tupol.flink.timeout.demo

import java.util.concurrent.TimeUnit

import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.async.AsyncCollector
import org.apache.flink.streaming.api.scala.{AsyncDataStream, DataStream, StreamExecutionEnvironment, _}

import scala.concurrent.duration._
import scala.util.Try


/**
 * Simple demo based on the Flink `AsyncDataStream` implementation.
 */
object AsyncDataStreamOrderedDemo extends DemoStreamProcessor with OutputFile {

  /** Main program method */
  def main(args: Array[String]) : Unit = {

    // get the execution environment
    val senv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    senv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val (file, timeout) = args match {
      case Array(file) => (file, 1000L)
      case Array(file, timeout) => (file, timeout.toLong)
      case _ => sys.error("Incorrect parameters; expected file path and an optional timeout in millis")
    }

    val inputRecords = recordsFromFile(file).toSeq
    val inputStream = senv.fromCollection(inputRecords).assignTimestampsAndWatermarks(RecordTimestampExtractor)

    // Setup the actual demo
    demoStreamProcessor(inputStream, outputFile(), timeout)

    // Setup the actual demo
    senv.execute(s"${this.getClass.getSimpleName}")

  }

  /**
   * Actual demo logic.
   *
   * @param inputStream
   * @param outputFile
   */
  def demoStreamProcessor(inputStream: DataStream[Record], outputFile: String, timeoutMs: Long = 1000): Unit = {

    // Trigger some time consuming operations on the stream
    val heavyWorkStream = heavyWorkStreamADS(inputStream.setParallelism(4), 1 second)

    heavyWorkStream.
      writeAsText(outputFile, WriteMode.OVERWRITE).setParallelism(1)
  }

  def heavyWorkStreamADS(inputStream: DataStream[Record], timeout: Duration) =
    AsyncDataStream.orderedWait(inputStream, timeout.toMillis, TimeUnit.MILLISECONDS, 10) {
      (in, collector: AsyncCollector[String]) =>
        val result = Try(timeConsumingOperation(in))
        collector.collect(Seq(s"$result"))
    }

}
