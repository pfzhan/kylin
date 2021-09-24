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

package io.kyligence.kap.engine.spark.job.stage.build.mlp

import io.kyligence.kap.engine.spark.job.SegmentJob
import io.kyligence.kap.engine.spark.job.stage.BuildParam
import io.kyligence.kap.metadata.cube.model.NDataSegment

class MLPRefreshColumnBytes(jobContext: SegmentJob, dataSegment: NDataSegment, buildParam: BuildParam)
  extends MLPBuildStage(jobContext, dataSegment, buildParam) {
  override def execute(): Unit = {
    // Refresh column bytes.
    tryRefreshColumnBytes()
    // By design, refresh layout bucket-num mapping.
    // Bucket here is none business of multi-level partition.
    tryRefreshBucketMapping()
    // Drain results, shutdown pool, cleanup extra immediate outputs.
    cleanup()
    logInfo(s"Finished SEGMENT $segmentId")
  }
}
