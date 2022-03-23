/*
 * Copyright (C) 2016 Kyligence Inc. All rights reserved.
 *
 * http://kyligence.io
 *
 * This software is the confidential and proprietary information of
 * Kyligence Inc. ("Confidential Information"). You shall not disclose
 * such Confidential Information and shall use it only in accordance
 * with the terms of the license agreement you entered into with
 * Kyligence Inc.
 *
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
 */

package io.kyligence.kap.query.asyncprofiler

import java.io.{File, OutputStream}
import java.nio.charset.Charset
import java.nio.file.Files
import java.util.concurrent.{CountDownLatch, TimeUnit}

import org.apache.commons.io.FileUtils
import org.apache.kylin.common.KylinConfig
import org.apache.kylin.common.exception.{KylinException, QueryErrorCode}
import org.apache.kylin.common.util.ZipFileUtils
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparderEnv


object AsyncProfiling extends Logging {

  import Message._

  private val localCacheDir = Files.createTempDirectory("ke-async-profiler-result-").toFile
  localCacheDir.deleteOnExit()
  private val resultCollectionTimeout = KylinConfig.getInstanceFromEnv.asyncProfilingResultTimeout
  private val profilingTimeout = KylinConfig.getInstanceFromEnv.asyncProfilingProfileTimeout
  private var timeoutExecutionThread: Thread = _

  private var command: String = createDriverMessage(NOP)
  private var running = false
  private var dumped = false
  private var cachedResult: CountDownLatch = _

  def nextCommand(): String = {
    command
  }

  def start(params: String): Unit = {
    val startParam = Option(params).getOrElse("start,event=cpu")
    AsyncProfiling.synchronized {
      if (running) {
        throw new KylinException(QueryErrorCode.PROFILING_ALREADY_STARTED, "profiling is already started, stop it first")
      }
      logDebug("profiler start")
      cleanLocalCache()
      // expecting driver + count(executor) amount of results
      cachedResult = new CountDownLatch(
        SparderEnv.getSparkSession.sparkContext.getExecutorMemoryStatus.size
      )
      logDebug(s"expecting ${cachedResult.getCount} to be collected")

      running = true
      dumped = false
      command = createDriverMessage(START, startParam) // inform executors
      AsyncProfilerTool.start(startParam) // start driver prof
      logDebug("profiler started")

      timeoutExecutionThread = new Thread(() => {
        try {
          Thread.sleep(profilingTimeout)
        } catch {
          case _: InterruptedException =>
            logTrace("profiler stopped normally, timeout thread interrupted and exit")
            Thread.currentThread().interrupt()
        }

        if (!Thread.interrupted()) {
          logInfo(s"profiling timeout after ${profilingTimeout}ms, stopping profiling")
          AsyncProfiling.synchronized {
            if (running) {
              running = false
              command = createDriverMessage(STOP)
              AsyncProfilerTool.stop()
            }
          }
        }
      })
      timeoutExecutionThread.start()
    }
  }

  def dump(params: String): Unit = {
    val dumpParam = Option(params).getOrElse("flamegraph")
    AsyncProfiling.synchronized {
      if (dumped) {
        throw new KylinException(QueryErrorCode.PROFILER_ALREADY_DUMPED, "result is already dumped.")
      }

      if (running) {
        logDebug("profiler stop")
        running = false
        timeoutExecutionThread.interrupt()
      } else {
        logWarning("profiling is not started")
      }

      dumped = true
      command = createDriverMessage(DUMP, dumpParam) // inform executors
      cacheDriverResult(AsyncProfilerTool.dump(dumpParam)) // dump driver prof
    }
  }

  def waitForResult(outStream: OutputStream): Unit = {
    if (!cachedResult.await(resultCollectionTimeout, TimeUnit.MILLISECONDS)) {
      logWarning(s"timeout while waiting for profile result")
    }
    logDebug(s"profiler stopped and result dumped to $localCacheDir")
    ZipFileUtils.compressZipFile(localCacheDir.getAbsolutePath, outStream)
  }

  private def suffix(content: String): String = {
    if (content.startsWith("<!DOCTYPE html>")) {
      ".html"
    } else {
      ""
    }
  }

  private[asyncprofiler] def cacheExecutorResult(content: String, executorId: String): Unit = {
    cacheResult(content, s"executor-$executorId${suffix(content)}")
    logDebug(s"cached result from executor-$executorId")
    cachedResult.countDown()
  }

  private[asyncprofiler] def cacheDriverResult(content: String): Unit = {
    cacheResult(content, s"driver${suffix(content)}")
    logDebug(s"cached result from driver")
    cachedResult.countDown()
  }

  private def cacheResult(content: String, destPath: String): Unit = {
    val path = s"${localCacheDir.getAbsolutePath}/$destPath"
    try {
      Files.write(new File(path).toPath, content.getBytes(Charset.defaultCharset()))
    } catch {
      case e: Exception =>
        logError("error writing dumped data to disk", e)
    }
  }

  private def cleanLocalCache(): Unit = {
    try {
      FileUtils.cleanDirectory(localCacheDir)
    } catch {
      case e: Exception =>
        logError("error clean cache directory", e)
    }
  }
}
