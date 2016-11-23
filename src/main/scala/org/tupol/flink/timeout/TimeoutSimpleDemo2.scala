package org.tupol.flink.timeout

import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

import scala.util.Try

import utils._

/**
 * Simple demo based on the initial raw idea for using async await, but this time bundling the original initial
 * record with the attempt to collect the result of hte time consuming operation.
  */
object TimeoutSimpleDemo2 extends DemoStreamProcessor with OutputFile {

  /** Main program method */
  def main(args: Array[String]) : Unit = {

    // get the execution environment
    val senv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    senv.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime)

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
  override def demoStreamProcessor(inputStream: DataStream[Record], outputFile: String): Unit = {

    // Trigger some time consuming operations
    val heavyWorkStream: DataStream[(Record, Try[(Record, String)])] = inputStream
      .map{ record =>
        import scala.concurrent._
        import ExecutionContext.Implicits.global
        import scala.concurrent.duration._
        lazy val action = future { (record, timeConsumingOperation(record.time)) }
        (record, Try{Await.result(action, 3 second)})
      }
      .setParallelism(4)

    heavyWorkStream.writeAsText(s"$outputFile-1", WriteMode.OVERWRITE).setParallelism(1)

    val joinedStream = inputStream.join(heavyWorkStream).where(_.key).equalTo(_._1.key)
      .window(TumblingProcessingTimeWindows.of(Time.seconds(2)))
      .apply{ (a, b) => (a, b._2) }
      .setParallelism(2)

    joinedStream
      .setParallelism(1)
      .writeAsText(outputFile, WriteMode.OVERWRITE).setParallelism(1)

  }
}