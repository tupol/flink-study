package org.tupol.flink.utils

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.util.TestStreamEnvironment
import org.apache.flink.test.util.TestEnvironment
import org.scalatest.{BeforeAndAfterEach, Suite}

/**
 *
 */
trait FlinkEnvironmentsSpec extends FlinkCusterSpec with BeforeAndAfterEach {
  this: Suite =>

  //StreamingMultipleProgramsTestBase

  private var _senv: StreamExecutionEnvironment =  _
  private var _env: ExecutionEnvironment =  _

  def senv = _senv
  def env = _env

  override def beforeEach(): Unit = {
    super.beforeEach()
    val _senv = new TestStreamEnvironment(cluster, parallelism)
    TestStreamEnvironment.setAsContext(cluster, parallelism)
    val _env = new TestEnvironment(cluster, parallelism)
    _env.setAsContext
  }

  override def afterEach(): Unit = {
    if(_senv != null) _senv = null
    if(_env != null) _env = null
    super.afterEach()
  }

}
