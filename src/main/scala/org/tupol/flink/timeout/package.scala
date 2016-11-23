package org.tupol.flink

import org.apache.flink.api.common.functions.RichMapFunction

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

}
