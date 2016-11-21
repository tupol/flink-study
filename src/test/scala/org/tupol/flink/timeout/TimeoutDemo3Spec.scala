package org.tupol.flink.timeout

import java.io.File
import java.util.UUID

import org.apache.flink.streaming.api.scala._
import org.scalatest.concurrent.Eventually
import org.scalatest.time.{Millis, Span}
import org.scalatest.{FunSuite, Matchers}
import org.tupol.flink.utils.{FlinkEnvironmentsSpec, TestDirectorySpec}

class TimeoutDemo3Spec extends FunSuite with Matchers with Eventually with FlinkEnvironmentsSpec with TestDirectorySpec {

  implicit override val patienceConfig = PatienceConfig(timeout = scaled(Span(6000, Millis)))

  test("Simple test for TimeoutDemo3") {

    val inputRecords: Seq[Record] = Seq(Record("a", 1000), Record("a", 1900), Record("b", 2100))
    val inputStream = senv.fromCollection(inputRecords)
    val outputFile = new File(testDir, UUID.randomUUID().toString).getAbsolutePath
    TimeoutDemo3.demoStreamProcessor(inputStream, outputFile)

    senv.execute()

    eventually{
      val result = env.readTextFile(outputFile).collect()
      println(s"===========================================")
      result.foreach(x => println(s"########### $x"))

      result.size shouldBe 3
      result.filter(line => line.contains("Success") && (line.contains("1000") || line.contains("1900")))
      result.filter(line => line.contains("Failure") && line.contains("2100") )
    }

    val result = env.readTextFile(outputFile).collect()
    println(s"===========================================")
    result.foreach(x => println(s"########### $x"))

  }

}
