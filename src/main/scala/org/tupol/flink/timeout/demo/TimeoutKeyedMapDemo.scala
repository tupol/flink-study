package org.tupol.flink.timeout.demo

import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.tupol.flink.timeout.TimeoutKeyedMap

import scala.concurrent.duration._
import scala.util.Try

/**
 * Simple demo based on `TimeoutKeyedMap`
  */
object TimeoutKeyedMapDemo extends DemoStreamProcessor with OutputFile {

  /** Main program method */
  def main(args: Array[String]) : Unit = {

    // get the execution environment
    val senv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    senv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // Setup the actual demo
    demoStreamProcessor(createRandomRecordsStream(senv, 100), outputFile(args))

    // Setup the actual demo
    senv.execute(s"${this.getClass.getSimpleName}")

  }

  /**
   * Actual demo logic.
   *
   * @param inputStream
   * @param outputFile
   */
  override def demoStreamProcessor(inputStream: DataStream[Record], outputFile: String): Unit = {
    // Trigger some time consuming operations
    val heavyWorkStream: DataStream[(String, Try[String])] = inputStream
      .map(record => (record.key, record))
      .map(TimeoutKeyedMap[String, Record, String](2 seconds){ in: Record => timeConsumingOperation(in.time) })
      .setParallelism(4)

    heavyWorkStream
      .writeAsText(outputFile, WriteMode.OVERWRITE)

  }
}
