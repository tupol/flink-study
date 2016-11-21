package org.tupol.flink.timeout

import java.io.File
import java.util.UUID

import org.scalatest.concurrent.Eventually
import org.scalatest.{FunSuite, Matchers}
import org.tupol.flink.utils.{FlinkEnvironmentsSpec, TestDirectorySpec}
import org.apache.flink.streaming.api.scala._

class TimeoutDemo4Spec extends FunSuite with Matchers with Eventually with FlinkEnvironmentsSpec with TestDirectorySpec {


  test("Simple test for TimeoutDemo4") {

    val inputRecords: Seq[Record] = Seq(Record("a", 1900), Record("b", 2100))
    val inputStream = senv.fromCollection(inputRecords)
    val outputFile = new File(testDir, UUID.randomUUID().toString).getAbsolutePath
    TimeoutDemo4.demoStreamProcessor(inputStream, outputFile)

    senv.execute()

    eventually{
      val result = env.readTextFile(outputFile).collect()
      result.size shouldBe 2
      result.filter(line => line.contains("1900") && line.contains("Success"))
      result.filter(line => line.contains("2100") && line.contains("Failure"))
    }
  }

}
