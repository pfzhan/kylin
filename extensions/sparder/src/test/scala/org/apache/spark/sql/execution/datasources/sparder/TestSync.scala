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

package org.apache.spark.sql.execution.datasources.sparder

import java.util.concurrent.{Executors, TimeUnit}

class TestSyncSuite {
  var count = 0

  def syncc(): Unit = {
    println("hi")
    synchronized {
      println("hi" + Thread.currentThread().getName)
      Thread.sleep(1000000)
      count = count + 1
    }
  }

  def runTask(): Unit = {
    val service = Executors.newFixedThreadPool(2)
    service.submit(new Task)
    service.submit(new Task)
    service.submit(new Task)
    service.submit(new Task)
    service.shutdown()
    service.awaitTermination(10000000, TimeUnit.DAYS)
  }

  class Task extends Runnable {
    override def run(): Unit = {
      syncc()
    }
  }

}


object TestSyncSuite extends App {
  new TestSyncSuite().runTask()
}
