package org.tupol.flink.timeout.demo

import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

import scala.util.Try

/**
 * Simple demo based on the initial raw idea for using async await.
  */
object TimeoutSimpleDemo1 extends DemoStreamProcessor with OutputFile {

  /** Main program method */
  def main(args: Array[String]) : Unit = {

    // get the execution environment
    val senv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    senv.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime)

    // Setup the actual demo
    demoStreamProcessor(createRandomRecordsStream(senv), outputFile(args))

    // Execute the demo
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
    val heavyWorkStream: DataStream[Try[(Record, String)]] = inputStream
      .map{ record =>
        import scala.concurrent._
        import ExecutionContext.Implicits.global
        import scala.concurrent.duration._
        import scala.util.Try
        lazy val action = future { (record, timeConsumingOperation(record.time)) }
        Try{Await.result(action, 2 second)}
      }
      .setParallelism(4)

    heavyWorkStream.writeAsText(s"$outputFile-1", WriteMode.OVERWRITE).setParallelism(1)

    val joinedStream = inputStream.join(heavyWorkStream.filter(_.isSuccess).map(_.get)).where(_.key).equalTo(_._1.key)
      .window(TumblingProcessingTimeWindows.of(Time.seconds(2)))
      .apply{ (a, b) => (a, b) }
      .setParallelism(2)

    joinedStream
      .writeAsText(outputFile, WriteMode.OVERWRITE)
  }
}
