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

    // Setup the actual demo
    demoStreamProcessor(createRandomRecordsStream(senv), outputFile(args))

    // Setup the actual demo
    senv.execute(s"${this.getClass.getSimpleName}")

  }

  /**
   * Actual demo logic.
   *
   * @param inputStream
   * @param outputFile
   */
  def demoStreamProcessor(inputStream: DataStream[Record], outputFile: String): Unit = {

    // Trigger some time consuming operations on the stream
    val heavyWorkStream = inputStream
      .map(TimeoutMap[Record, String](2 seconds){ in: Record => timeConsumingOperation(in.time) })
      .setParallelism(4)

    heavyWorkStream
      .setParallelism(1).
      writeAsText(outputFile, WriteMode.OVERWRITE)
  }

}
