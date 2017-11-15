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

import java.io.InputStream
import java.util.concurrent.TimeUnit

import com.google.common.cache.{Cache, CacheBuilder, RemovalListener, RemovalNotification}
import com.google.common.io.CountingInputStream
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataOutputStream, Path}
import org.apache.hadoop.io.IOUtils
import org.apache.kylin.common.KylinConfig
import org.apache.kylin.common.persistence.ResourceStore
import org.apache.kylin.metadata.model.DataModelManager
import org.apache.spark.internal.Logging

object SparderDictionaryManager extends Logging {

  lazy val store: ResourceStore =
    DataModelManager.getInstance(KylinConfig.getInstanceFromEnv).getStore
  lazy val baseDictPath = new Path(
    KylinConfig.getInstanceFromEnv.getHdfsWorkingDirectory + "/sparder")

  lazy val conf = new Configuration()
  val dictionaryCache: Cache[String, String] = CacheBuilder.newBuilder
    .maximumSize(10)
    .expireAfterWrite(1, TimeUnit.HOURS)
    .removalListener(new RemovalListener[String, String]() {
      override def onRemoval(
          notification: RemovalNotification[String, String]): Unit = {
        val deletePath = new Path(notification.getValue)
        logInfo(s"remove dictionary from hdfs: $deletePath")
        deletePath.getFileSystem(conf).deleteOnExit(deletePath)

      }
    })
    .build
    .asInstanceOf[Cache[String, String]]

  def add(dictPath: String): String = {
    val dict = dictionaryCache.getIfPresent(dictPath)
    if (dict != null) {
      dict
    } else {
      addDictToHdfs(dictPath)
    }
  }

  def addDictToHdfs(dictPath: String): String = {
    val hdfsDictPath = Path.mergePaths(baseDictPath, new Path(dictPath))
    val system = hdfsDictPath.getFileSystem(conf)
    if (system.exists(hdfsDictPath)) {
      system.delete(hdfsDictPath, false)
    }
    var out: FSDataOutputStream = null
    var in: InputStream = null
    try {
      out = system.create(hdfsDictPath)
      in = new CountingInputStream(store.getResource(dictPath).inputStream)
      IOUtils.copyBytes(in, out, 4096)
      out.hflush()
      val fileSize = in.asInstanceOf[CountingInputStream].getCount
      dictionaryCache.put(dictPath, hdfsDictPath.toString)
      logInfo(
        s"add dictionary $dictPath to hdfs: $hdfsDictPath,dict size :${fileSize}")
    } finally {
      if (out != null) out.close()
      if (in != null) in.close()
    }
    hdfsDictPath.toString
  }
}
