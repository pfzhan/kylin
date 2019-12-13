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

package org.apache.spark.application

import java.util.concurrent.Executors

import io.kyligence.kap.engine.spark.application.SparkApplication
import io.kyligence.kap.engine.spark.scheduler._
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.KylinJobEventLoop

class JobWorker(application: SparkApplication, args: Array[String], eventLoop: KylinJobEventLoop) extends Logging {
  private val pool = Executors.newSingleThreadExecutor()

  eventLoop.registerListener(new KylinJobListener {
    override def onReceive(event: KylinJobEvent): Unit = {
      event match {
        case _: RunJob => runJob()
        case _ =>
      }
    }
  })

  def stop(): Unit = {
    pool.shutdownNow()
    application.logJobInfo()
  }

  private def runJob(): Unit = {
    execute()
  }


  private def execute(): Unit = {
    pool.execute(new Runnable {
      override def run(): Unit = {
        try {
          logInfo("Start running job.")
          application.execute(args)
          eventLoop.post(JobSucceeded())
        } catch {
          case exception: NoRetryException => eventLoop.post(UnknownThrowable(exception))
          case throwable: Throwable => eventLoop.post(ResourceLack(throwable))
        }
      }
    })
  }
}

class NoRetryException(msg: String) extends java.lang.Exception(msg) {
  def this() {
    this(null)
  }
}