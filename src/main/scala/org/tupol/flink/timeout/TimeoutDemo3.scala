package org.tupol.flink.timeout

import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

import scala.concurrent.duration._
import scala.util.Try


/**
 * Simple demo based on `TimeoutKeyedMap`
  */
object TimeoutDemo3 extends DemoStreamProcessor with OutputFile {

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
  override def demoStreamProcessor(inputStream: DataStream[Record], outputFile: String): Unit = {
    // Trigger some time consuming operations
    val asyncStream: DataStream[(String, Try[String])] = inputStream
      .map(record => (record.key, record))
      .map(TimeoutKeyedMap[String, Record, String](2 seconds){ in: Record => timeConsumingOperation(in.time) })
      .setParallelism(4)

    asyncStream.writeAsText(s"$outputFile-1", WriteMode.OVERWRITE).setParallelism(1)

    val joinedStream = inputStream.join(asyncStream).where(_.key).equalTo(_._1)
      .window(TumblingProcessingTimeWindows.of(Time.seconds(2)))
      .apply{ (a, b) => (a, b._2) }
      .setParallelism(2)

    joinedStream.writeAsText(outputFile, WriteMode.OVERWRITE).setParallelism(1)
  }
}
