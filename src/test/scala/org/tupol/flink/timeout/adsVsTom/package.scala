package org.tupol.flink.timeout

import java.io.File
import java.util.concurrent.TimeUnit

import org.apache.commons.io.FileUtils
import org.apache.flink.streaming.api.scala.async.AsyncCollector
import org.apache.flink.streaming.api.scala.{AsyncDataStream, DataStream, _}
import org.tupol.flink.timeout.demo._

import scala.collection.JavaConversions._
import scala.concurrent.duration.Duration
import scala.io.Source
import scala.util.Try

/**
 * Basic testing tools
 */
package object adsVsTom {

  val ADSN_OUT_FILE = "/tmp/adsn.out"
  val ADSO_OUT_FILE = "/tmp/adso.out"
  val TMAP_OUT_FILE = "/tmp/tmap.out"

  def timeConsumingOperation(record: Record): String = {
    Thread.sleep(record.time)
    f"[${record.key}%6s | ${record.time}%6d]"
  }

  def heavyWorkStreamADSOrdered(inputStream: DataStream[Record], timeout: Duration) =
    AsyncDataStream.orderedWait(inputStream, timeout.toMillis, TimeUnit.MILLISECONDS, 10) {
      (in, collector: AsyncCollector[String]) =>
        val result = Try(timeConsumingOperation(in))
        collector.collect(Seq(s"$result"))
    }

  def heavyWorkStreamADSUnordered(inputStream: DataStream[Record], timeout: Duration) =
    AsyncDataStream.unorderedWait(inputStream, timeout.toMillis, TimeUnit.MILLISECONDS, 10) {
      (in, collector: AsyncCollector[String]) =>
        val result = Try(timeConsumingOperation(in))
        collector.collect(Seq(s"$result"))
    }

  def heavyWorkStreamTOM(inputStream: DataStream[Record], timeout: Duration) =
    inputStream.map(TimeoutMap[Record, String](timeout)(timeConsumingOperation) )


  def linesFromFlinkFile(path: String): Iterable[String] = {
    val file = new File(path)
    if(file.isDirectory)
      FileUtils.listFiles(file, null, false).flatMap(f => Source.fromFile(f).getLines)
    else
      Source.fromFile(file).getLines.toIterable
  }

  def linesFromFlinkFiles(path: String): Iterable[Iterator[String]] = {
    val file = new File(path)
    if(file.isDirectory)
      FileUtils.listFiles(file, null, false).map(f => Source.fromFile(f).getLines)
    else
      Iterable(Source.fromFile(file).getLines)
  }

}
