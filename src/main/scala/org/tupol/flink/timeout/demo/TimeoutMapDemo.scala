package org.tupol.flink.timeout.demo

import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.tupol.flink.timeout.TimeoutMap

import scala.concurrent.duration._

/**
 * Simple demo based on `TimeoutMap`
  */
object TimeoutMapDemo extends DemoStreamProcessor with OutputFile {


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
  def demoStreamProcessor(inputStream: DataStream[Record], outputFile: String, timeoutMs: Long): Unit = {
    // Trigger some time consuming operations on the stream
    val heavyWorkStream = inputStream
      .setParallelism(4)
      .map( TimeoutMap[Record, String](timeoutMs millis)( timeConsumingOperation ) )

    heavyWorkStream
      .setParallelism(1)
      .writeAsText(outputFile, WriteMode.OVERWRITE)
  }

}
