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
package io.kyligence.kap.common

import org.apache.spark.utils.KafkaTestUtils

import java.util.{Calendar, Date, Locale, TimeZone}
import scala.io.Source

trait StreamingDataGenerate extends KafkaTestUtils {

  var start = System.currentTimeMillis()
  var totalMsgCount = 0

  def generate(path: String, topic: String): Unit = {
    sendOneRound(path, topic, 10)
  }

  def generate(path: String, topic: String, qps: Int, loop: Boolean): Unit = {
    if (loop) {
      while (true) {
        sendOneRound(path, topic, qps)
      }
    } else {
      sendOneRound(path, topic, qps)
    }
  }

  def generate(path: String, topic: String, count: Int): Unit = {
    val lines = Source.fromFile(path).getLines()
//    val lines = Source.fromFile("/Users/binbin.zheng/select.txt").getLines().mkString

    var cnt = 0;
    val iter = lines.toIterator
    while (iter.hasNext && cnt < count) {
      val line = iter.next()
      val currentTimeStamp = System.currentTimeMillis()
      val cal = Calendar.getInstance(TimeZone.getDefault, Locale.getDefault(Locale.Category.FORMAT))
      cal.setTime(new Date(currentTimeStamp))
      cal.set(Calendar.SECOND, 0)
      cal.set(Calendar.MILLISECOND, 0)
      val msg = cal.getTime.getTime + "," + line
      totalMsgCount += 1
      sendMessages(topic, Map(msg -> 1))
      cnt = cnt + 1
    }
    flush()

  }

  def sendOneRound(path: String, topic: String, qps: Int): Unit = {
    val content = Source.fromFile(path).getLines().foreach { line =>
      val currentTimeStamp = System.currentTimeMillis()
      val cal = Calendar.getInstance(TimeZone.getDefault, Locale.getDefault(Locale.Category.FORMAT))
      cal.setTime(new Date(currentTimeStamp))
      cal.set(Calendar.SECOND, 0)
      cal.set(Calendar.MILLISECOND, 0)
      val msg = cal.getTime.getTime + "," + line
      totalMsgCount += 1
      sendMessages(topic, Map(msg -> 1))
      if (totalMsgCount % qps == 0) {
        val interval = System.currentTimeMillis() - start
        logInfo(s"send ${StreamingTestConstant.KAFKA_QPS} massege cost ${interval}")
        logInfo(s"total send msg count is ${totalMsgCount}")
        if (interval < 1000) {
          Thread.sleep(1000 - interval)
        }
        start = System.currentTimeMillis()
      }
    }
    flush()
  }
}
