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

package io.kyligence.kap.engine.spark.job;

import static io.kyligence.kap.job.execution.stage.StageType.MERGE_COLUMN_BYTES;
import static io.kyligence.kap.job.execution.stage.StageType.MERGE_FLAT_TABLE;
import static io.kyligence.kap.job.execution.stage.StageType.MERGE_INDICES;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;

import com.google.common.base.Throwables;

import io.kyligence.kap.engine.spark.job.exec.MergeExec;
import io.kyligence.kap.engine.spark.job.stage.BuildParam;
import io.kyligence.kap.metadata.cube.model.NDataSegment;
import lombok.val;

public class SegmentMergeJob extends SegmentJob {

    @Override
    protected String generateInfo() {
        return LogJobInfoUtils.dfMergeJobInfo();
    }

    @Override
    protected final void doExecute() throws Exception {
        merge();
    }

    private void merge() throws IOException {
        Stream<NDataSegment> segmentStream = config.isSegmentParallelBuildEnabled() ? //
                readOnlySegments.parallelStream() : readOnlySegments.stream();
        AtomicLong finishedSegmentCount = new AtomicLong(0);
        val segmentsCount = readOnlySegments.size();
        segmentStream.forEach(seg -> {
            try (KylinConfig.SetAndUnsetThreadLocalConfig autoCloseConfig = KylinConfig
                    .setAndUnsetThreadLocalConfig(config)) {
                val jobStepId = StringUtils.replace(infos.getJobStepId(), JOB_NAME_PREFIX, "");
                val exec = new MergeExec(jobStepId);

                val buildParam = new BuildParam();
                MERGE_FLAT_TABLE.createStage(this, seg, buildParam, exec);
                MERGE_INDICES.createStage(this, seg, buildParam, exec);

                exec.mergeSegment();

                val mergeColumnBytes = MERGE_COLUMN_BYTES.createStage(this, seg, buildParam, exec);
                mergeColumnBytes.toWorkWithoutFinally();

                if (finishedSegmentCount.incrementAndGet() < segmentsCount) {
                    mergeColumnBytes.onStageFinished(true);
                }
            } catch (IOException e) {
                Throwables.propagate(e);
            }
        });
    }

    public static void main(String[] args) {
        SegmentMergeJob segmentMergeJob = new SegmentMergeJob();
        segmentMergeJob.execute(args);
    }
}
