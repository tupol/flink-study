package org.tupol.flink.timeout

import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import utils._

import scala.util.Try

/**
  * This program connects to a server socket and reads strings from the socket.
  * The easiest way to try this out is to open a text sever (at port 9999)
  * using the <i>netcat</i> tool via
  * <pre>
  * nc -kl 9999
  * </pre>
  * and run this example with the port as an argument.
  */
object SimpleJoinDemo extends DemoStreamProcessor with OutputFile {

  /** Main program method */
  def main(args: Array[String]) : Unit = {

    // get the execution environment
    val senv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    senv.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime)

    // Setup the actual demo
    demoStreamProcessor(createRecordsFromStringSocketStream(senv), outputFile(args))

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
    val heavyWorkStream: DataStream[(Record, Try[String])] = inputStream
      .map{ record => (record, Try(timeConsumingOperation(record.time))) }
      .setParallelism(4)
      .keyBy(0)

    heavyWorkStream.writeAsText(s"$outputFile-1", WriteMode.OVERWRITE).setParallelism(1)

    val joinedStream = inputStream.join(heavyWorkStream.filter(_._2.isSuccess)).where(_.key).equalTo(_._1.key)
      .window(TumblingProcessingTimeWindows.of(Time.seconds(2)))
      .apply{ (a, b) => (a, b) }
      .setParallelism(2)

    joinedStream
      .setParallelism(1)
      .writeAsText(outputFile, WriteMode.OVERWRITE).setParallelism(1)
  }
}
