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

package io.kyligence.kap.streaming.common

import java.util.concurrent.atomic.AtomicLong

import org.apache.spark.sql.SparkSession
import io.kyligence.kap.metadata.cube.model.NDataSegment
import java.util.{List => JList}

class MergeJobEntry(
       spark: SparkSession,
       project: String,
       dataflowId: String,
       val afterMergeSegmentSourceCount: Long,
       val globalMergeTime: AtomicLong,
       val unMergedSegments: JList[NDataSegment],
       val afterMergeSegment: NDataSegment) extends JobEntry(project, dataflowId, spark) {
  override def toString: String = {
    s"""
       |==========================[MEGER JOB INFO]===============================
       | project: ${project}
       | dataflowId: ${dataflowId}
       | unMergedSegments: ${unMergedSegments.toString()}
       | afterMergeSegment:${afterMergeSegment.toString}
       |==========================[MERGE JOB INFO]===============================
     """.stripMargin
  }

}
