/*
 * Copyright (C) 2016 Kyligence Inc. All rights reserved.
 * http://kyligence.io
 * This software is the confidential and proprietary information of
 * Kyligence Inc. ("Confidential Information"). You shall not disclose
 * such Confidential Information and shall use it only in accordance
 * with the terms of the license agreement you entered into with
 * Kyligence Inc.
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 */
package org.apache.spark.sql.common

// scalastyle:off

import java.io.File
import java.util.{Locale, TimeZone}

import org.apache.commons.io.FileUtils
import org.apache.log4j.spi.LoggingEvent
import org.apache.log4j.{Appender, AppenderSkeleton, Level, Logger}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.plans.SQLHelper
import org.apache.spark.util.{AccumulatorContext, Utils}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, BeforeAndAfterEach, Outcome}

import scala.annotation.tailrec
import scala.collection.mutable.ArrayBuffer

trait SparderBaseFunSuite
  extends AnyFunSuite
    with BeforeAndAfterAll
    with BeforeAndAfterEach
    with SQLHelper
    with Logging {
  // scalastyle:on

  // Initialize the logger forcibly to let the logger log timestamp
  // based on the local time zone depending on environments.
  // The default time zone will be set to America/Los_Angeles later
  // so this initialization is necessary here.
  log

  // Timezone is fixed to America/Los_Angeles for those timezone sensitive tests (timestamp_*)
  TimeZone.setDefault(TimeZone.getTimeZone("America/Los_Angeles"))
  // Add Locale setting
  Locale.setDefault(Locale.US)

  protected val enableAutoThreadAudit = true

  protected override def beforeAll(): Unit = {
    super.beforeAll()
  }

  protected override def afterAll(): Unit = {
    try {
      // Avoid leaking map entries in tests that use accumulators without SparkContext
      AccumulatorContext.clear()
    } finally {
      super.afterAll()
    }
  }

  // helper function
  protected final def getTestResourceFile(file: String): File = {
    new File(getClass.getClassLoader.getResource(file).getFile)
  }

  protected final def getTestResourcePath(file: String): String = {
    getTestResourceFile(file).getCanonicalPath
  }

  protected final def copyAndGetResourceFile(fileName: String, suffix: String): File = {
    val url = Thread.currentThread().getContextClassLoader.getResource(fileName)
    // To avoid illegal accesses to a resource file inside jar
    // (URISyntaxException might be thrown when accessing it),
    // copy it into a temporary one for accessing it from the dependent module.
    val file = File.createTempFile("test-resource", suffix)
    file.deleteOnExit()
    FileUtils.copyURLToFile(url, file)
    file
  }

  /**
   * Note: this method doesn't support `BeforeAndAfter`. You must use `BeforeAndAfterEach` to
   * set up and tear down resources.
   */
  def testRetry(s: String, n: Int = 2)(body: => Unit): Unit = {
    test(s) {
      retry(n) {
        body
      }
    }
  }

  /**
   * Note: this method doesn't support `BeforeAndAfter`. You must use `BeforeAndAfterEach` to
   * set up and tear down resources.
   */
  def retry[T](n: Int)(body: => T): T = {
    if (this.isInstanceOf[BeforeAndAfter]) {
      throw new UnsupportedOperationException(
        s"testRetry/retry cannot be used with ${classOf[BeforeAndAfter]}. " +
          s"Please use ${classOf[BeforeAndAfterEach]} instead.")
    }
    retry0(n, n)(body)
  }

  @tailrec private final def retry0[T](n: Int, n0: Int)(body: => T): T = {
    try body
    catch {
      case e: Throwable =>
        if (n > 0) {
          logWarning(e.getMessage, e)
          logInfo(s"\n\n===== RETRY #${n0 - n + 1} =====\n")
          // Reset state before re-attempting in order so that tests which use patterns like
          // LocalSparkContext to clean up state can work correctly when retried.
          afterEach()
          beforeEach()
          retry0(n - 1, n0)(body)
        }
        else throw e
    }
  }

  /**
   * Log the suite name and the test name before and after each test.
   *
   * Subclasses should never override this method. If they wish to run
   * custom code before and after each test, they should mix in the
   * {{org.scalatest.BeforeAndAfter}} trait instead.
   */
  final protected override def withFixture(test: NoArgTest): Outcome = {
    val testName = test.text
    val suiteName = this.getClass.getName
    val shortSuiteName = suiteName.replaceAll("org.apache.spark", "o.a.s")
    try {
      logInfo(s"\n\n===== TEST OUTPUT FOR $shortSuiteName: '$testName' =====\n")
      test()
    } finally {
      logInfo(s"\n\n===== FINISHED $shortSuiteName: '$testName' =====\n")
    }
  }

  /**
   * Creates a temporary directory, which is then passed to `f` and will be deleted after `f`
   * returns.
   */
  protected def withTempDir(f: File => Unit): Unit = {
    val dir = Utils.createTempDir()
    try f(dir) finally {
      Utils.deleteRecursively(dir)
    }
  }

  /**
   * Adds a log appender and optionally sets a log level to the root logger or the logger with
   * the specified name, then executes the specified function, and in the end removes the log
   * appender and restores the log level if necessary.
   */
  protected def withLogAppender(
                                 appender: Appender,
                                 loggerName: Option[String] = None,
                                 level: Option[Level] = None)(
                                 f: => Unit): Unit = {
    val logger = loggerName.map(Logger.getLogger).getOrElse(Logger.getRootLogger)
    val restoreLevel = logger.getLevel
    logger.addAppender(appender)
    if (level.isDefined) {
      logger.setLevel(level.get)
    }
    try f finally {
      logger.removeAppender(appender)
      if (level.isDefined) {
        logger.setLevel(restoreLevel)
      }
    }
  }

  class LogAppender(msg: String = "", maxEvents: Int = 1000) extends AppenderSkeleton {
    val loggingEvents = new ArrayBuffer[LoggingEvent]()

    override def append(loggingEvent: LoggingEvent): Unit = {
      if (loggingEvents.size >= maxEvents) {
        val loggingInfo = if (msg == "") "." else s" while logging $msg."
        throw new IllegalStateException(
          s"Number of events reached the limit of $maxEvents$loggingInfo")
      }
      loggingEvents.append(loggingEvent)
    }

    override def close(): Unit = {}

    override def requiresLayout(): Boolean = false
  }

}
