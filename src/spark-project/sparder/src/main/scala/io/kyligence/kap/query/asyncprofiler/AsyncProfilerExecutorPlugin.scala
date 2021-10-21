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

import java.util
import java.util.concurrent.{Executors, TimeUnit}

import com.google.common.util.concurrent.ThreadFactoryBuilder
import org.apache.spark.api.plugin.{ExecutorPlugin, PluginContext}
import org.apache.spark.internal.Logging

class AsyncProfilerExecutorPlugin extends ExecutorPlugin with Logging {

  private val checkingInterval: Long = 1000
  private var ctx: PluginContext = _
  private var dumped = false

  private val scheduledExecutorService = Executors.newScheduledThreadPool(1,
    new ThreadFactoryBuilder().setDaemon(true).setNameFormat("profiler-%d").build())

  override def init(_ctx: PluginContext, extraConf: util.Map[String, String]): Unit = {
    ctx = _ctx
    val profile = new Runnable {
      override def run(): Unit = checkAndProfile()
    }
    log.debug(s"AsyncProfiler status: ${AsyncProfilerTool.status()}")
    scheduledExecutorService.scheduleWithFixedDelay(
      profile, 0, checkingInterval, TimeUnit.MILLISECONDS)
  }

  def checkAndProfile(): Unit = {
    import Message._
    try {
      val reply = ask(createExecutorMessage(NEXT_COMMAND, ctx.executorID()))
      val (command, _, param) = Message.processMessage(reply)
      command match {
        case START if !AsyncProfilerTool.running() =>
          dumped = false
          AsyncProfilerTool.start(param)
        case STOP if AsyncProfilerTool.running() =>
          AsyncProfilerTool.stop()
        case DUMP if !dumped => // doesn't allow a second dump for simplicity
          val result = AsyncProfilerTool.dump(param)
          AsyncProfilerTool.stop() // call stop anyway to make sure profiler is stopped
          dumped = true
          send(createExecutorMessage(RESULT, ctx.executorID(), result))
        case _ =>
      }
    } catch {
      case e: Exception =>
        logInfo("error while communication/profiling", e)
    }
  }

  private def ask(msg: String): String = {
    logTrace(s"ask: $msg")
    val result = ctx.ask(msg).toString
    logTrace(s"ask result: $result")
    result
  }

  private def send(msg: String): Unit = {
    logTrace(s"send: $msg")
    ctx.send(msg)
  }

}
