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
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.kyligence.kap.engine.spark.builder

import java.io.IOException
import java.util

import io.kyligence.kap.metadata.cube.model.NDataSegment
import org.apache.kylin.metadata.model.TblColRef
import org.apache.spark.SparkContext

import scala.collection.JavaConverters._

object NGlobalDictionaryBuilderAssist {

  @throws[IOException]
  def resize(col: TblColRef, seg: NDataSegment, bucketPartitionSize: Int, sc: SparkContext): Unit = {
    val globalDict = new NGlobalDictionaryV2(seg.getProject, col.getTable, col.getName, seg.getConfig.getHdfsWorkingDirectory)
    val broadcastDict = sc.broadcast(globalDict)
    val paralRdd = sc.parallelize(Range(0, bucketPartitionSize), bucketPartitionSize)

    globalDict.prepareWrite()

    val rd = paralRdd.flatMap { bucketId =>
      val gDict: NGlobalDictionaryV2 = broadcastDict.value
      val bucketDict: NBucketDictionary = gDict.loadBucketDictionary(bucketId)
      val tuple2List = new util.ArrayList[(String, Long)](bucketDict.getAbsoluteDictMap.size)
      for (entry <- bucketDict.getAbsoluteDictMap.object2LongEntrySet.asScala) {
        val tuple2 = (entry.getKey, entry.getLongValue)
        tuple2List.add(tuple2)
      }
      tuple2List.asScala.iterator
    }.partitionBy(new NHashPartitioner(bucketPartitionSize))
      .mapPartitionsWithIndex({
        case (bucketId, iterator) =>
          val gDict = broadcastDict.value
          val bucketDict = gDict.createNewBucketDictionary()

          while (iterator.hasNext) {
            val tuple2 = iterator.next
            bucketDict.addAbsoluteValue(tuple2._1, tuple2._2)
          }

          bucketDict.saveBucketDict(bucketId)

          Iterator.empty
      }).count

    globalDict.writeMetaDict(bucketPartitionSize,
      seg.getConfig.getGlobalDictV2MaxVersions, seg.getConfig.getGlobalDictV2VersionTTL)
  }
}
