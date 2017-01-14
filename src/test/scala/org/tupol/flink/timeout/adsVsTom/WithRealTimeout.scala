package org.tupol.flink.timeout.adsVsTom

import java.io.File

import org.apache.commons.io.FileUtils
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}
import org.tupol.flink.timeout.demo._

import scala.concurrent.duration._

/**
 * Run some simple tests as following:
 * 1. Run a test that produces writes the transformed input stream to a file using the TimeoutMap
 * 2. Run a test that produces writes the transformed input stream to a file using the AsyncDataStream calling orderedWait
 * 4. Run a test that produces writes the transformed input stream to a file using the AsyncDataStream calling unorderedWait
 * 4. Compare the files and runtimes
 *
 * Not exactly a unit test, but hey... it serves a purpose.
 *
 * A! Important! In this one some task might be taking longer than the timeout so we are expecting timeouts.
 */
class WithRealTimeout extends FunSuite with Matchers with BeforeAndAfterAll {

  val TIMEOUT = 100 milli

  var adsnRuntime: Long = _
  var adsoRuntime: Long = _
  var tmapRuntime: Long = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    FileUtils.deleteQuietly(new File(ADSN_OUT_FILE))
    FileUtils.deleteQuietly(new File(ADSO_OUT_FILE))
    FileUtils.deleteQuietly(new File(TMAP_OUT_FILE))
  }

  val inputRecords = generateRandomRecords(size = 1000, maxDuration = TIMEOUT.toMillis + 300)

  test("TOM Runtime") {

    val senv: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironment()
    senv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //senv.enableCheckpointing(1000, CheckpointingMode.EXACTLY_ONCE)

    val inputStream = senv.fromCollection(inputRecords).assignTimestampsAndWatermarks(RecordTimestampExtractor)

    heavyWorkStreamTOM(inputStream, TIMEOUT).writeAsText(TMAP_OUT_FILE, WriteMode.OVERWRITE)

    tmapRuntime = timeCode(senv.execute("TimeoutMap Async I/O job"))._2

  }

  test("ADS OrderedWait Runtime") {

    val senv: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironment()
    senv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //senv.enableCheckpointing(1000)

    val inputStream: DataStream[Record] = senv.fromCollection(inputRecords).assignTimestampsAndWatermarks(RecordTimestampExtractor)

    heavyWorkStreamADSOrdered(inputStream, TIMEOUT).writeAsText(ADSO_OUT_FILE, WriteMode.OVERWRITE)

    adsoRuntime = timeCode(senv.execute("AsyncDataStream Async I/O job"))._2

  }

  test("ADS UnorderedWait Runtime") {

    val senv: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironment()
    senv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //senv.enableCheckpointing(1000)

    val inputStream: DataStream[Record] = senv.fromCollection(inputRecords).assignTimestampsAndWatermarks(RecordTimestampExtractor)

    heavyWorkStreamADSUnordered(inputStream, TIMEOUT).writeAsText(ADSN_OUT_FILE, WriteMode.OVERWRITE)

    adsnRuntime = timeCode(senv.execute("AsyncDataStream Async I/O job"))._2

  }

  test("Output Differences") {

    println(
      f"""
         |-------------------------------------------------------
         |Runtime for the TimeoutMap:                    $tmapRuntime%4d ms
         |Runtime for the AsyncDataStream orderedWait:   $adsoRuntime%4d ms
         |Runtime for the AsyncDataStream unorderedWait: $adsnRuntime%4d ms
         |-------------------------------------------------------
         |
       """.stripMargin)

    val linesADSO = linesFromFlinkFile(ADSO_OUT_FILE)
    val linesADSN = linesFromFlinkFile(ADSN_OUT_FILE)
    val linesTMAP = linesFromFlinkFile(TMAP_OUT_FILE)

    tmapRuntime shouldBe < (adsoRuntime)
    tmapRuntime shouldBe < (adsnRuntime)

    linesADSO.foldLeft(false)((acc, line) => acc || line.contains("Failure")) shouldBe false
    linesADSN.foldLeft(false)((acc, line) => acc || line.contains("Failure")) shouldBe false
    linesTMAP.foldLeft(false)((acc, line) => acc || line.contains("Failure")) shouldBe true

    linesADSO shouldNot be (linesTMAP)
    //TODO Find out why the ordered and unordered wait calls produce the same output here
    linesADSN shouldBe linesADSO
  }

}


