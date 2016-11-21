package org.tupol.flink.utils

import org.apache.flink.test.util._
import org.scalatest.{BeforeAndAfterAll, Suite}



trait FlinkCusterSpec extends BeforeAndAfterAll {
  this: Suite =>

  def parallelism: Int = 4

  private var _cluster: ForkableFlinkMiniCluster =  _

  def cluster = _cluster

  override def beforeAll(): Unit = {
    super.beforeAll()
    _cluster = TestBaseUtils.startCluster(1, parallelism, false, false, true)
  }

  override def afterAll(): Unit = {
    TestBaseUtils.stopCluster(_cluster, TestBaseUtils.DEFAULT_TIMEOUT)
    super.afterAll()
  }

}
