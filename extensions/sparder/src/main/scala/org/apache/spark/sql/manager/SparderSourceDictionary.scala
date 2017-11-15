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
package org.apache.spark.sql.manager

import java.util.concurrent.TimeUnit

import com.google.common.cache.{
  Cache,
  CacheBuilder,
  RemovalListener,
  RemovalNotification
}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.kylin.dict.{DictionaryInfo, DictionaryInfoSerializer}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.manager.ResourceLock.Lock

object SparderSourceDictionary extends Logging {
  lazy val conf = new Configuration
  lazy val serializer = new DictionaryInfoSerializer()
  lazy val resourceLock = new ResourceLock
  val dictionaryCache: Cache[String, DictionaryInfo] = CacheBuilder.newBuilder
    .maximumSize(10000)
    .expireAfterWrite(1, TimeUnit.HOURS)
    .removalListener(new RemovalListener[String, DictionaryInfo]() {
      override def onRemoval(
          notification: RemovalNotification[String, DictionaryInfo]): Unit = {}
    })
    .build
    .asInstanceOf[Cache[String, DictionaryInfo]]

  def getOrCreate(dictionaryPath: String): DictionaryInfo = {
    val dictionary = dictionaryCache.getIfPresent(dictionaryPath)
    if (dictionary != null) {
      dictionary
    } else {
      loadDictionary(dictionaryPath)
    }
  }

  def loadDictionary(dictionaryPath: String): DictionaryInfo = {
    val startTime = System.currentTimeMillis()
    var lock: Lock = null
    try {
      lock = resourceLock.getLockInterna(dictionaryPath)
    } catch {
      case e: Exception =>
        log.info(e.getMessage)
    }
    val dictionary = dictionaryCache.getIfPresent(dictionaryPath)
    if (dictionary != null) {
      if (lock != null) {
        lock.release()
      }
      logInfo(
        s"load cache dictionary time :${System.currentTimeMillis() - startTime} ")
      dictionary
    } else {
      val dictionary = new Path(dictionaryPath)
      val fileSystem = dictionary.getFileSystem(conf)
      val stream = fileSystem.open(dictionary)
      val dictionaryInfo = serializer.deserialize(stream)
      dictionaryCache.put(dictionaryPath, dictionaryInfo)
      if (lock != null) {
        lock.release()
      }
      logInfo(
        s"load dictionary time :${System.currentTimeMillis() - startTime} ")
      dictionaryInfo
    }
  }
}
