package org.tupol.flink

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

import scala.util.Try

package object timeout {

  /**
   * A RichMapFunction function that takes a timeout parameter and a function from value to output.
   *
   * The actual map function works on values and returns a tuple of input and try of mapping input to output.
   *
   * When the time runs out, the mapped value will be a failure of `java.util.concurrent.TimeoutException`.
   *
   * @param timeout how long the value mapping function should run before timing out.
   * @param f the mapping function from input input to output
   * @tparam I the input value type
   * @tparam O the output type
   */
  case class TimeoutMap[I, O](timeout: scala.concurrent.duration.Duration)(f: I => O)
    extends RichMapFunction[I, (I, Try[O])] {
    override def map(input: I): (I, Try[O]) = {
      import scala.concurrent._
      import ExecutionContext.Implicits.global
      ( input, Try { Await.result(future(f(input)), timeout) } )
    }
  }

  /**
   * A RichMapFunction function that takes a timeout parameter and a function from value to output.
   *
   * The actual map function works on key-value pairs and returns a tuple of key and try of mapping value to output.
   *
   * When the time runs out, the mapped value will be a failure of `java.util.concurrent.TimeoutException`.
   *
   * @param timeout how long the value mapping function should run before timing out.
   * @param f the mapping function from input value to output
   * @tparam K the input key type
   * @tparam V the input value type
   * @tparam O the output type
   */
  case class TimeoutKeyedMap[K, V, O](timeout: scala.concurrent.duration.Duration)(f: V => O)
    extends RichMapFunction[(K, V), (K, Try[O])] {
    override def map(input: (K, V)): (K, Try[O]) = {
      import scala.concurrent._
      import ExecutionContext.Implicits.global
      ( input._1, Try { Await.result(future(f(input._2)), timeout) } )
    }
  }

  /**
   * Define a simple type for our records, having a string key and a random number, which we will use to
   * start a "timeConsumingOperation" and for all the demos in this package.
   * @param key
   * @param time
   */
  case class Record(key: String, time: Long)


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
    val inputRecords: Seq[Record] = (0 to size) map { x => Record(f"key_$x%03d", scala.util.Random.nextInt(5000)) }
    env.fromCollection(inputRecords)
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

}
