package org.tupol.flink.utils

import java.io.File
import java.util.UUID

import org.apache.commons.io.FileUtils
import org.scalatest.{BeforeAndAfterEach, Suite}

/**
 * Setup test directory and silently delete them after each test.
 */
trait TestDirectorySpec extends BeforeAndAfterEach {
  this: Suite =>

  private var _testDir: File = _

  def testDir = _testDir


  override def beforeEach(): Unit = {
    super.beforeEach()
    val uuid = UUID.randomUUID
    _testDir = new File(s"/tmp/test-$uuid")
    _testDir.mkdirs()
  }

  override def afterEach(): Unit = {
    super.afterEach()
    FileUtils.deleteDirectory(testDir)
  }
}
