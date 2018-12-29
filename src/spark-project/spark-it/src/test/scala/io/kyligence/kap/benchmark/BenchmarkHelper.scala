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

package io.kyligence.kap.benchmark

import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.{ExecutorService, Executors, Semaphore}

import org.apache.spark.internal.Logging

class BenchmarkHelper(threadNum: Int, interval: Long) extends Logging {
  private val totalQueryCount = new AtomicLong(0)
  private var tasks = Seq.empty[Runnable]
  private val pool: ExecutorService = Executors.newFixedThreadPool(threadNum)
  private var monitorRunner: Runnable = _
  private val semaphore = new Semaphore(threadNum)
  private var logQPSInterval = 10L
  private var startTime: Long = _

  def runTest(): Unit = {
    startTime = System.currentTimeMillis()
    new Thread(monitorRunner).start()
    new Thread(new LogQPS).start()
    if (tasks.isEmpty) {
      logWarning("Tasks is Empty!")
    } else {
      var counter = 0
      val startTime = System.currentTimeMillis()
      try {
        while (System.currentTimeMillis() - startTime <= interval) {
          semaphore.acquire()
          pool.submit(tasks.apply(counter % tasks.length))
          counter += 1
        }
      } finally {
        pool.shutdown()
      }
    }
  }

  def setTasks(functions: Array[() => Any]): Unit = {
    tasks = functions.map(new RunTask(_))
  }

  def setMonitor(monitor : () => Unit) : Unit = {
    monitorRunner = new Runnable {
      override def run(): Unit = monitor.apply()
    }
  }
  def setLogQPSInterval(seconds: Long): Unit = {
    logQPSInterval = seconds
  }


  class RunTask(f: () => Any) extends Runnable {
    override def run(): Unit = {
      f.apply()
      totalQueryCount.incrementAndGet()
      semaphore.release()
    }
  }

  class LogQPS() extends Runnable {
    override def run(): Unit = {
      var lastQueryCount = 0L
      while (true) {
        Thread.sleep(logQPSInterval * 1000)
        println(s"QPS: ${(totalQueryCount.get() - lastQueryCount) * 1.0 / logQPSInterval} ")
        println(s"Avg QPS: ${(totalQueryCount.get()) * 1.0 / ((System.currentTimeMillis() - startTime) /1000)} ")
        lastQueryCount = totalQueryCount.get()
      }
    }
  }

}
