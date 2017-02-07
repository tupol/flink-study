package org.tupol.flink.timeout

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.watermark.Watermark

import scala.util.Try

/**
 * Basic utilities to make life easier
 */
package object demo {

  /**
   * Basic demo function
   */
  trait DemoStreamProcessor {
    def demoStreamProcessor(inputStream: DataStream[Record], outputFile: String): Unit
  }

  /**
   * Some mock time consuming operation that will run for a defined time.
   *
   * @param time approximate duration of the operation
   * @return a string containing the time and a data/time interval
   */
  def timeConsumingOperation(time: Long): String = {
    import java.util.Date
    val from = new Date().toString
    Thread.sleep(time)
    val to = new Date().toString
    f"[$time%6d | $from - $to]"
  }

  /**
   * Record DataStream factory from a collection of `length` size.
   *
   * @param env
   * @param size
   * @return
   */
  def createRandomRecordsStream(env: StreamExecutionEnvironment, size: Int = 20): DataStream[Record] = {
    val inputRecords: Seq[Record] = (0 to size) map { x => Record(f"key_$x%03d",
                                                                  scala.util.Random.nextInt(5000),
                                                                  System.currentTimeMillis() + 2 * x) }
    env.fromCollection(inputRecords).assignTimestampsAndWatermarks(RecordTimestampExtractor)
  }

  /**
   *
   * Record DataStream factory by reading from a network socket specified by `host` and `port`.
   *
   * @param env
   * @param host
   * @param port
   * @return
   */
  def createRecordsFromStringSocketStream(env: StreamExecutionEnvironment, host: String = "localhost", port: Int = 9999): DataStream[Record] = {
    // get input data by connecting to the socket
    val inputStream: DataStream[String] = env.socketTextStream(host, port, '\n')
    inputStream map { line => val r = line.split("\\s"); Record(r(0), r(1).toInt) }
  }

  trait OutputFile {
    def outputFile(args: Array[String]): String = {
      val defaultFile = s"/tmp/${this.getClass.getSimpleName.replace("$", "")}.out"
      Option(Try { ParameterTool.fromArgs(null).get("out") }.toOption).flatten.getOrElse(defaultFile)
    }
  }

  case object RecordTimestampExtractor extends AssignerWithPeriodicWatermarks[Record] with Serializable {
    override def extractTimestamp(e: Record, prevElementTimestamp: Long) = e.timestamp
    override def getCurrentWatermark(): Watermark = new Watermark(System.currentTimeMillis)
  }

}
