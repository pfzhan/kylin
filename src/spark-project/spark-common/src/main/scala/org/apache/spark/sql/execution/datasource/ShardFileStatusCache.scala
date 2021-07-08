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
package org.apache.spark.sql.execution.datasource

import com.google.common.cache.{Cache, CacheBuilder}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.datasources.FileStatusCache

import java.util.concurrent.atomic.AtomicReference

object ShardFileStatusCache {
   var fsc: AtomicReference[FileStatusCache] = new AtomicReference[FileStatusCache]
   val segmentBuildTimeCache: Cache[String, java.lang.Long] = CacheBuilder.newBuilder().build()

   def getFileStatusCache(session: SparkSession): FileStatusCache = {
      if (fsc.get() == null) {
         fsc.set(FileStatusCache.getOrCreate(session))
      }
      fsc.get()
   }

   def getSegmentBuildTime(segmentId: String): Long = {
      val cacheTime = segmentBuildTimeCache.getIfPresent(segmentId)
      if (cacheTime == null) -1 else cacheTime
   }

   def refreshSegmentBuildTimeCache(segmentId: String, newBuildTime: Long): Unit = {
      segmentBuildTimeCache.put(segmentId, newBuildTime)
   }
}
