package org.tupol.flink.utils

import java.io.File

import org.apache.commons.io.FileUtils
import org.scalatest.{BeforeAndAfterEach, Suite}

/**
 * Setup test directory and silently delete them after each test.
 */
trait TestFileSpec extends BeforeAndAfterEach {
  this: Suite =>

  private var _testFile: String = _

  def testFile = _testFile


  override def beforeEach(): Unit = {
    super.beforeEach()
    _testFile = File.createTempFile("result-path", "dir").toURI().toString()
  }

  override def afterEach(): Unit = {
    super.afterEach()
    FileUtils.deleteDirectory(new File(testFile))
  }
}
