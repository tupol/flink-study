package org.tupol.flink.timeout

import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._

import scala.concurrent.duration._


/**
 * Simple demo based on `TimeoutMap`
  */
object TimeoutDemo4 extends DemoStreamProcessor with OutputFile {


  /** Main program method */
  def main(args: Array[String]) : Unit = {

    // get the execution environment
    val senv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    senv.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime)

    // Setup the actual demo
    demoStreamProcessor(createRecordsStreamFromCollection(senv), outputFile(args))

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
    val asyncStream = inputStream
      .map(TimeoutMap[Record, String](2 seconds){ in: Record => timeConsumingOperation(in.time) })
      .setParallelism(4)

    asyncStream.writeAsText(outputFile, WriteMode.OVERWRITE).setParallelism(1)
  }

}
