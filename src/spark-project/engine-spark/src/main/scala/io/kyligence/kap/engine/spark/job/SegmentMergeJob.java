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

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import com.google.common.base.Preconditions;

import io.kyligence.kap.metadata.cube.model.NDataSegment;

public class SegmentMergeJob extends SegmentJob {

    @Override
    protected String generateInfo() {
        return LogJobInfoUtils.dfMergeJobInfo();
    }

    @Override
    protected final void doExecute() throws Exception {
        if (isMLP()) {
            mergeMLP();
        } else {
            merge();
        }
    }

    private void merge() throws IOException {
        for (NDataSegment dataSegment : readOnlySegments) {
            SegmentMergeExec exec = new SegmentMergeExec(this, dataSegment);
            exec.mergeSegment();
        }
    }

    private void mergeMLP() throws IOException {
        for (NDataSegment dataSegment : readOnlySegments) {
            SegmentMergeExec exec = new MLPMergeExec(this, dataSegment);
            exec.mergeSegment();
        }
    }

    protected final List<NDataSegment> getUnmergedSegments(NDataSegment merged) {
        List<NDataSegment> unmerged = dataflowManager.getDataflow(dataflowId).getMergingSegments(merged);
        Preconditions.checkNotNull(unmerged);
        Preconditions.checkState(!unmerged.isEmpty());
        Collections.sort(unmerged);
        return unmerged;
    }

    public static void main(String[] args) {
        SegmentMergeJob segmentMergeJob = new SegmentMergeJob();
        segmentMergeJob.execute(args);
    }
}
