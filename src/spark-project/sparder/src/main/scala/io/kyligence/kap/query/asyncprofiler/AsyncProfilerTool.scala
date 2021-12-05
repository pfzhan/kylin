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

import org.apache.spark.internal.Logging

/**
 * this class is not thread safe
 */
object AsyncProfilerTool extends Logging {

  private val profiler = AsyncProfiler.getInstance()

  private var _running = false

  def running(): Boolean = {
    _running
  }

  def start(params: String): Unit = {
    try {
      log.debug("start profiling")
      _running = true
      profiler.execute(params)
    } catch {
      case e: Exception =>
        log.error("failed start profiling", e)
    }
  }

  def dump(params: String): String = {
    log.debug("stop and dump profiling")
    try {
      _running = false
      profiler.execute(params)
    } catch {
      case e: Exception =>
        log.error("failed dump profiling", e)
        e.toString
    }
  }

  def stop(): Unit = {
    try {
      _running = false
      profiler.stop()
    } catch {
      case e: Exception =>
        logTrace("failed stop profiling", e)
    }
  }

  def status(): String = {
    profiler.execute("status")
  }

  def execute(cmd: String): String = {
    try {
      profiler.execute(cmd)
    } catch {
      case e: Exception =>
        log.error("failed exec profiling", e)
        ""
    }
  }
}
